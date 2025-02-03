/*
Copyright 2024 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scalers

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	awsutils "github.com/kedacore/keda/v2/pkg/scalers/aws"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

/*

 To emit the single metric in getMetricsAndActiviy(), this scaler upses multiple kafka metrics and data points
 from the brokers.  All those internal metrics rely on a time interval, for this reason and due to the 'liverly'
 nature of kakfka consumer offsets metrics, thought the scaler will adapt to SO cofiguration, it will produce best results
 with a polling interval of a minute or more with HPA defaul config of --horizontal-pod-autoscaler-sync-period 15 seconds.

 Scaler logic assumes metricType: Value and useCachedMetrics as below, it makes scaling decisions at the consumer group
 level, metricType: Average does not apply.

spec:
  pollingInterval: 60
    triggers:
  - type: kafka-streams
    metricType: Value
    useCachedMetrics: true
*/

// Kafka metrics evaluated for each topic partition in the consumer group
type kafkaPartitionMetrics struct {
	writeRate, readRate float64 // Rates in msg/millisecond
	lag                 int64   // standard Kafka lag, in number of messages
	residualLag         int64   // measure of avegrage expected lag in a consumer group that consumes messages close to 'as they are produced', in number of messages
	lagRatio            float64 // no unit, lag/residualLag
}

// Kafka metrics evaluated for each topic in the consumer group
type kafkaTopicMetrics struct {
	writeRate, readRate float64 // Rates in msg/millisecond
	lag, residualLag    int64   // in number of messages
	lagRatio            float64 // no unit, lag/residualLag
	period              int64   // number of milliseconds over which above rates are calculated,  with useCachedMetrics: true, it should be close to pollingInterval
	partitionsWithLag   int64   // # topic partitions with a measurable lag
	partitionsTotal     int64   // # total partitions in the topic
}

type kafkaStreamsScaler struct {
	metricType v2.MetricTargetType
	metadata   *kafkaStreamsMetadata // scaler config from coarse validation of user input + default values
	client     *kafka.Client         // kafka-go client
	logger     logr.Logger
	// Scaler state
	previousConsumerOffsets map[string]map[int]int64     // committed offsets for all the topics and topic parttions in last poll
	previousLastOffsets     map[string]map[int]int64     // last offsets for all the topics and topic parttions in last poll
	lastOffetsTime          int64                        // timestamp where all those offsets were updated
	topicMetrics            map[string]kafkaTopicMetrics // Calculated metrics for each topic used for scaling decisions
	writesRollingAvg        map[string]float64           // Approximate write/s rolling avg
	aboveThresholdCount     map[string]int64             // Consecutive polling periods where lagRatio is met for 'MeasurementsForScale' for scale up
	underThreasholdCount    int64                        //C onsecutive polling periods where lagRatio is met for 'MeasurementsForScale' for scale down
	groupState              string                       // last poll consumer group state
	groupMembersCount       int64                        // Number of members in the consumer group
	groupHosts              int64                        // Number of hpsts in the consumer group
	lastScaleUpTopicName    string                       // Store the name of the last topic for which metrics caused a scale up
	lastScaleUpMetrics      *kafkaTopicMetrics           // Store metrics of the last topic that casued scale up
	pollingCount            int64                        // Times the getMetricsAndActivity() API was called by keda.
	pollingStableCount      int64                        // Times the getMetricsAndActivity() API was called by keda and a metric could be calculated
}

/*
 This scaler keeps track internally of the number of polling intervals where metric is above target with MeasurementsForScale
 It would be nice to use HPA policies, but 'periodSeconds' and 'stabilizationWindowSeconds' semantic is different, rolling
 maximum is a different behavior thatn emitting a single metric that will cause scale up/down'.  Better decision can
 be made in the scaler rater than emitting a metric at ever polloing interval and counting on policies and rolling max for final decisions.
 This scaler will produce best results with disablinng cooldown and set periodsSeconds under pollingInterval like this:
 spec:
  pollingInterval: 60
  initialCooldownPeriod: 60
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 0
          selectPolicy: Max
          policies:
            - type: Percent
              value: 100
              periodSeconds: 60
            - type: Pods
              value: 1
              periodSeconds: 60

*/

type LimitScaleUp int

const (
	noLimit LimitScaleUp = iota
	topicLimit
	groupLimit
)

const (
	noPartitionOffset = int64(-1)
	// default Values for trigger parameters
	defaultLagRatio                          = 3.0        // no unit
	defaultCommitInterval                    = 30000      // milliseconds, default commit interval in Java Kafka streaming library.
	defaultMinPartitionWriteThrouput         = 0.5        // in msg/secs.  lagRatio will not be calculated when throuhput is lower than this value
	defaultMeasurementsForScale              = 3          // number of polling intervals where conditions for scale up/down are met before scaling action
	defaultScaleDownFactor                   = 0.60       // How much write rates to topic have to come down from last scale up to initiate downscale,
	defaultLimitToPartitionsWithLag          = true       // when true, average lagratio at the topic level ignoring partitions with no writes.
	defaultAllowedTimeLagCatchUp             = 600        // if consumerGroup is estimated to catchup lag under that time in seconds, do not scale up
	defaultWritesToReadTolerance             = 20         // Tolerance to decide if reads and writes are 'close' one another, in percentage
	defaultWritesToReadRatioDampening        = 0.66       // when calculating an HPA metric using the writes to read ratio, use a damping factor to avoid replicas overshoot
	defaultMinReadRateToUseForReplicasCount  = 10         // Do not use writes to consumer read ratio to estimate HPA metric if topic read rate is lower in msg/s
	defaultHPAMetricFactorMinimumScaleFactor = 1.11       // Target * 1.11 is just above HPA globally-configurable tolerance, 0.1 by default.
	defaultLimitScaleUp                      = groupLimit // Limit scaling if group Members would exceed partitions: "group" -> topic with max partitions, "topic" -> topic causing scaling up, "none"
	defaultMinPartitionsWithLag              = 2          // Do not scale unless the lag is on at least that many parition.
	// Not configuratble (yet) default parameters for scaling decision.
	scaleUpOnMultipleTopic = false // When true (not implemented!), scale on any combination of topic meeting threshold after MeasurementsForScale
)

type kafkaStreamsMetadata struct {
	// Madatory Metadata
	BootstrapServers []string
	Group            string
	// Optional Metadata with no default values
	Topic []string
	// Optional Metadata with default values
	LagRatio                          float64
	CommitInterval                    int64
	MinPartitionWriteThrouput         float64
	MeasurementsForScale              int64
	ScaleDownFactor                   float64
	LimitToPartitionsWithLag          bool
	AllowedTimeLagCatchUp             int64
	WritesToReadTolerance             int64
	WritesToReadRatioDampening        float64
	MinReadRateToUseForReplicasCount  int64
	HPAMetricFactorMinimumScaleFactor float64
	LimitScaleUp                      LimitScaleUp
	MinPartitionsWithLag              int64

	// Authenticaltion, copied from apache-kafka implementation
	// TODO: Not implemented!
	// SASL
	SASLType kafkaSaslType
	Username string
	Password string
	// MSK
	AWSRegion        string
	AWSEndpoint      string
	AWSAuthorization awsutils.AuthorizationMetadata
	// TLS
	TLS         string
	Cert        string
	Key         string
	KeyPassword string
	CA          string

	triggerIndex int
}

func (a *kafkaStreamsMetadata) enableTLS() bool {
	// TODO: Not implemented!  No authentication
	return false
	// return a.TLS == stringEnable
}

func parseKafkaStreamsMetadata(config *scalersconfig.ScalerConfig) (*kafkaStreamsMetadata, error) {
	meta := kafkaStreamsMetadata{}

	// Mandatory parameters with no default values
	if val, ok := config.TriggerMetadata["bootstrapServers"]; ok {
		servers := strings.Split(val, ",")
		meta.BootstrapServers = servers
	} else {
		return nil, fmt.Errorf("mandatory config missing: bootstrapServers")
	}
	if val, ok := config.TriggerMetadata["consumerGroup"]; ok {
		meta.Group = val
	} else {
		return nil, fmt.Errorf("mandatory config missing: consumerGroup")
	}

	// Optional parameters with no default values
	// Topic is generally not necessary
	if val, ok := config.TriggerMetadata["topic"]; ok {
		topics := strings.Split(val, ",")
		meta.Topic = topics
	}

	// Optional parameters with a default value
	if val, ok := config.TriggerMetadata["lagRatio"]; ok {
		lagRatio, err := strconv.ParseFloat(val, 64)
		if err != nil || lagRatio <= 1.0 {
			return nil, fmt.Errorf("lagRatio must be a float greater than 1.0")
		}
		meta.LagRatio = lagRatio
	} else {
		meta.LagRatio = defaultLagRatio
	}

	if val, ok := config.TriggerMetadata["commitInterval"]; ok {
		commitInterval, err := strconv.ParseInt(val, 10, 64)
		if err != nil || commitInterval <= 0 {
			return nil, fmt.Errorf("commitInterval must be an integer in milliseconds greater than 0")
		}
		meta.CommitInterval = commitInterval
	} else {
		meta.CommitInterval = defaultCommitInterval
	}
	if val, ok := config.TriggerMetadata["minPartitionWriteThrouput"]; ok {
		minThroughput, err := strconv.ParseFloat(val, 64)
		if err != nil || minThroughput <= 0.0 {
			return nil, fmt.Errorf("minPartitionWriteThrouput must be a float in bytes/second greater than 0")
		}
		meta.MinPartitionWriteThrouput = minThroughput
	} else {
		meta.MinPartitionWriteThrouput = defaultMinPartitionWriteThrouput
	}
	if val, ok := config.TriggerMetadata["measurementsForScale"]; ok {
		measurements, err := strconv.ParseInt(val, 10, 64)
		if err != nil || measurements <= 0 {
			return nil, fmt.Errorf("measurementsForScale must be a inteter number greater than 0")
		}
		meta.MeasurementsForScale = measurements
	} else {
		meta.MeasurementsForScale = defaultMeasurementsForScale
	}
	if val, ok := config.TriggerMetadata["scaleDownFactor"]; ok {
		scaleDown, err := strconv.ParseFloat(val, 64)
		if err != nil || scaleDown <= 0.0 || scaleDown >= 1.0 {
			return nil, fmt.Errorf("ScaleDownFactor must be a float number between 0 and 1.0")
		}
		meta.ScaleDownFactor = scaleDown
	} else {
		meta.ScaleDownFactor = defaultScaleDownFactor
	}
	if val, ok := config.TriggerMetadata["limitToPartitionsWithLag"]; ok {
		lagOnly, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("limitToPartitionsWithLag must be \"true\" or \"false\"")
		}
		meta.LimitToPartitionsWithLag = lagOnly
	} else {
		meta.LimitToPartitionsWithLag = defaultLimitToPartitionsWithLag
	}
	if val, ok := config.TriggerMetadata["allowedTimeLagCatchUp"]; ok {
		lagTime, err := strconv.ParseInt(val, 10, 64)
		if err != nil || lagTime <= 0 {
			return nil, fmt.Errorf("allowedTimeLagCatchUp must be a inteter number in seconds greater than 0")
		}
		meta.AllowedTimeLagCatchUp = lagTime
	} else {
		meta.AllowedTimeLagCatchUp = defaultAllowedTimeLagCatchUp
	}
	if val, ok := config.TriggerMetadata["writesToReadTolerance"]; ok {
		wt, err := strconv.ParseInt(val, 10, 64)
		if err != nil || wt < 0 || wt > 100 {
			return nil, fmt.Errorf("writesToReadTolerance must be a inteter percentage value between 0 and 100")
		}
		meta.WritesToReadTolerance = wt
	} else {
		meta.WritesToReadTolerance = defaultWritesToReadTolerance
	}
	if val, ok := config.TriggerMetadata["writesToReadRatioDampening"]; ok {
		wtrr, err := strconv.ParseFloat(val, 64)
		if err != nil || wtrr <= 0.0 || wtrr >= 1.0 {
			return nil, fmt.Errorf("writesToReadRatioDampening must be a float number between 0 and 1.0")
		}
		meta.WritesToReadRatioDampening = wtrr
	} else {
		meta.WritesToReadRatioDampening = defaultWritesToReadRatioDampening
	}
	if val, ok := config.TriggerMetadata["minReadRateToUseForReplicasCount"]; ok {
		mrt, err := strconv.ParseInt(val, 10, 64)
		if err != nil || mrt < 0 {
			return nil, fmt.Errorf("minReadRateToUseForReplicasCount must be a inteter greater than 0")
		}
		meta.MinReadRateToUseForReplicasCount = mrt
	} else {
		meta.MinReadRateToUseForReplicasCount = defaultMinReadRateToUseForReplicasCount
	}
	if val, ok := config.TriggerMetadata["hpaMetricFactorMinimumScaleFactor"]; ok {
		hpaMin, err := strconv.ParseFloat(val, 64)
		if err != nil || hpaMin < 1.0 {
			return nil, fmt.Errorf("hpaMetricFactorMinimumScaleFactor must be a float number greater or equal to 1.0")
		}
		meta.HPAMetricFactorMinimumScaleFactor = hpaMin
	} else {
		meta.HPAMetricFactorMinimumScaleFactor = defaultHPAMetricFactorMinimumScaleFactor
	}
	if val, ok := config.TriggerMetadata["limitScaleUp"]; ok {
		switch val {
		case "none":
			meta.LimitScaleUp = noLimit
		case "topic":
			meta.LimitScaleUp = topicLimit
		case "group":
			meta.LimitScaleUp = groupLimit
		default:
			return nil, fmt.Errorf("limitScaleUp must be one of \"none\", \"topic\", \"group\"")
		}
	} else {
		meta.LimitScaleUp = defaultLimitScaleUp
	}
	if val, ok := config.TriggerMetadata["minPartitionsWithLag"]; ok {
		mrt, err := strconv.ParseInt(val, 10, 64)
		if err != nil || mrt < 2 {
			return nil, fmt.Errorf("minPartitionsWithLag must be a inteter greater than 1")
		}
		meta.MinPartitionsWithLag = mrt
	} else {
		meta.MinPartitionsWithLag = defaultMinPartitionsWithLag
	}

	// TODO: parse Authentication (TLS, SASL,MSK).     Hardcoded to no SASL.
	meta.SASLType = KafkaSASLTypeNone

	// meta. meta.ScalerIndexIndex = config.ScalerIndex
	return &meta, nil
}

// NewkafkaStreamScaler -- creates a new kafkaStreamScaler
func NewKafkaStreamScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, err
	}
	meta, err := parseKafkaStreamsMetadata(config)
	if err != nil {
		return nil, err
	}
	logger := InitializeLogger(config, "kafka_streams_scaler")
	client, err := getKafkaGoClient(ctx, *meta, logger)
	if err != nil {
		return nil, err
	}

	previousConsumerOffsets := make(map[string]map[int]int64)
	previousLastOffsets := make(map[string]map[int]int64)
	topicMetrics := make(map[string]kafkaTopicMetrics)
	writesRollingAvg := make(map[string]float64)
	aboveThresholdCount := make(map[string]int64)

	return &kafkaStreamsScaler{
		client:                  client,
		metricType:              metricType,
		metadata:                meta,
		logger:                  logger,
		previousConsumerOffsets: previousConsumerOffsets,
		previousLastOffsets:     previousLastOffsets,
		topicMetrics:            topicMetrics,
		writesRollingAvg:        writesRollingAvg,
		aboveThresholdCount:     aboveThresholdCount,
		lastScaleUpTopicName:    "Not Set",
		lastScaleUpMetrics:      nil,
	}, nil
}

// initializes Kafka go client
func getKafkaGoClient(ctx context.Context, metadata kafkaStreamsMetadata, logger logr.Logger) (*kafka.Client, error) {
	var saslMechanism sasl.Mechanism
	var tlsConfig *tls.Config
	var err error

	logger.V(4).Info(fmt.Sprintf("Kafka SASL type %s", metadata.SASLType))
	if metadata.enableTLS() {
		tlsConfig, err = kedautil.NewTLSConfigWithPassword(metadata.Cert, metadata.Key, metadata.KeyPassword, metadata.CA, false)
		if err != nil {
			return nil, err
		}
	}

	switch metadata.SASLType {
	case KafkaSASLTypeNone:
		saslMechanism = nil
	case KafkaSASLTypePlaintext:
		saslMechanism = plain.Mechanism{
			Username: metadata.Username,
			Password: metadata.Password,
		}
	case KafkaSASLTypeSCRAMSHA256:
		saslMechanism, err = scram.Mechanism(scram.SHA256, metadata.Username, metadata.Password)
		if err != nil {
			return nil, err
		}
	case KafkaSASLTypeSCRAMSHA512:
		saslMechanism, err = scram.Mechanism(scram.SHA512, metadata.Username, metadata.Password)
		if err != nil {
			return nil, err
		}
	case KafkaSASLTypeOAuthbearer:
		return nil, errors.New("SASL/OAUTHBEARER is not implemented yet")
	case KafkaSASLTypeMskIam:
		cfg, err := awsutils.GetAwsConfig(ctx, metadata.AWSRegion, metadata.AWSAuthorization)
		if err != nil {
			return nil, err
		}

		saslMechanism = aws_msk_iam_v2.NewMechanism(*cfg)
	default:
		return nil, fmt.Errorf("err sasl type %q given", metadata.SASLType)
	}

	transport := &kafka.Transport{
		TLS:  tlsConfig,
		SASL: saslMechanism,
	}
	client := kafka.Client{
		Addr:      kafka.TCP(metadata.BootstrapServers...),
		Transport: transport,
	}
	if err != nil {
		return nil, fmt.Errorf("error creating kafka client: %w", err)
	}

	return &client, nil
}

// Scaler Interface -- GetMetricsAndActivity()
func (s *kafkaStreamsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	s.logger.V(0).Info("GetMetricsAndActivity")
	metricVal, err := s.getMetricForHPA(ctx)

	if err != nil {
		// log the reason of the failed metric calculation, do not return the error to Keda.
		s.logger.V(0).Info(fmt.Sprintf("HPA final, Metric = TARGET, no mesurement due to  %s", err))
	}

	// on errors, getMetricForHPA returns metric = TARGET
	metric := GenerateMetricInMili(metricName, metricVal)
	return []external_metrics.ExternalMetricValue{metric}, true, nil
}

// Scaler Interface -- GetMetricSpecForScaling()
// TODO consider using a better TARGET ?  TARGET is set at lagRatio threshold, however although LagRatio is a key element for scaling up,
// the final metric returned to HPA is NOT just measured LagRatio, it takes into account other internal metrics to emit a number that will
// have 'desired' effect on replicas count based on over streaming consumer group state

func (s *kafkaStreamsScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	metricName := fmt.Sprintf("kafka-streams-%s-topics", s.metadata.Group)
	metricTarget := s.metadata.LagRatio
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(metricName)),
		},
		Target: GetMetricTargetMili(s.metricType, metricTarget),
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: "External"}
	s.logger.V(2).Info(fmt.Sprintf("kafka-streams metric name: %s", metricName))
	return []v2.MetricSpec{metricSpec}
}

// Scaler Interface: Clos()
func (s *kafkaStreamsScaler) Close(context.Context) error {
	if s.client == nil {
		return nil
	}
	// TODO: from apache-kafka scaler, not tested, not sure this work
	transport := s.client.Transport.(*kafka.Transport)
	if transport != nil {
		transport.CloseIdleConnections()
	}
	return nil
}

// Update all consumer group, topic, partitions metrics, make scaling decsion, calculate the SO metric for HPA
func (s *kafkaStreamsScaler) getMetricForHPA(ctx context.Context) (float64, error) {
	s.pollingCount++ // internal stat
	hpaMetric := s.metadata.LagRatio
	err := s.getAllConsumerGroupMetrics(ctx)
	if err != nil {
		s.resetScalingMeasurementsCount()
		return hpaMetric, err
	}
	factor, scaleUpTargetMet, err := s.getScaleUpDecisionAndFactor()
	if err != nil {
		s.resetScalingMeasurementsCount()
		return hpaMetric, err
	}

	scaleDownTargetMet := false
	if !scaleUpTargetMet {
		factor, scaleDownTargetMet, err = s.getScaleDownDecisionAndFactor()
		if err != nil {
			s.resetScalingMeasurementsCount()
			return hpaMetric, err
		}
	}

	// Pick most relevant topic for logging/debugging only.
	topicInfoForLog := ""
	switch {
	case scaleUpTargetMet:
		a := int64(0)
		ratio := 0.0
		for name, cnt := range s.aboveThresholdCount {
			r := s.topicMetrics[name].lagRatio
			switch {
			case cnt > a:
				a = cnt
				topicInfoForLog = name
			case cnt == a:
				if r >= ratio {
					ratio = r
					topicInfoForLog = name
				}
			case cnt < a:
				// nothing
			}
		}
	case scaleDownTargetMet:
		topicInfoForLog = s.lastScaleUpTopicName
	default:
		ratio := 0.0
		for name, topicMetrics := range s.topicMetrics {
			if topicMetrics.lagRatio > ratio {
				ratio = topicMetrics.lagRatio
				topicInfoForLog = name
			}
		}
	}

	met := s.topicMetrics[topicInfoForLog]
	s.logger.V(0).Info(fmt.Sprintf("Final Metric: %.3f, Group state:%s, lag ratio: %.3f, counts up/down: %d/%d, lag: %d, residual lag: %d, write/s: %.1f, read/s: %.1f, write/s rolling avg: %.1f, group: %s on topic: %s",
		hpaMetric*factor, s.groupState, met.lagRatio, s.aboveThresholdCount[topicInfoForLog], s.underThreasholdCount, met.lag, met.residualLag, met.writeRate*1000, met.readRate*1000, s.writesRollingAvg[topicInfoForLog]*1000, s.metadata.Group, topicInfoForLog))
	if s.lastScaleUpMetrics != nil {
		s.logger.V(0).Info(fmt.Sprintf("Final Metric: last scale up topic: %s, write/s: %f", s.lastScaleUpTopicName, s.lastScaleUpMetrics.writeRate*1000))
	}
	return hpaMetric * factor, nil
}

func withinPercentage(num1, num2, percentage float64) bool {
	diff := math.Abs(num1 - num2)
	threshold := (percentage / 100) * math.Max(math.Abs(num1), math.Abs(num2))

	return diff <= threshold
}

func (s *kafkaStreamsScaler) resetScalingMeasurementsCount() {
	for name := range s.aboveThresholdCount {
		s.aboveThresholdCount[name] = 0
	}
}

// Update topics write/s rolling average over N periods, with N windown size = MeasurementsForScale
func (s *kafkaStreamsScaler) updateRollingAvg() {
	// no rates on first iteration, down insert 0 in rolling average.
	if s.pollingStableCount < 1 {
		s.pollingStableCount++
		return
	}
	cnt := math.Min(float64(s.pollingStableCount), float64(s.metadata.MeasurementsForScale))
	for name := range s.writesRollingAvg {
		old_avg := s.writesRollingAvg[name]
		// basic formula for calculating rolling average with just last avg and window size
		s.writesRollingAvg[name] = (old_avg * (cnt - 1) / cnt) + (s.topicMetrics[name].writeRate / float64(cnt))
	}
	s.logger.V(2).Info(fmt.Sprintf("Rolling: %v pool:%d ", s.writesRollingAvg, s.pollingStableCount))
	s.pollingStableCount++
}

/*
 Returns a scale up factor >= 1.0 that will multiply the target metric.
 HPA formula:
	desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]

 desiredReplicas DOES NOT direclty becomes HPA 'desired replicas count', it HPA policies contribute.
*/

func (s *kafkaStreamsScaler) getScaleUpDecisionAndFactor() (scaleFactor float64, scaleUpTargetMet bool, err error) {
	scaleFactor = 1.0
	scaleUpTargetMet = false
	topicName := "" // name of the most relevant topic in the consumer group when reaching a scaling decision point
	topicWrites := 0.0
	scaleUpCount := int64(0)

	// No scaling action unless the consumer group is 'Stable', reset all counts and re-start measuring when stable.
	if s.groupState != "Stable" {
		s.resetScalingMeasurementsCount()
		s.logger.V(0).Info(fmt.Sprintf("Reset measurements counts for group: %s in state %s", s.metadata.Group, s.groupState))
		return scaleFactor, scaleUpTargetMet, nil
	}

	s.updateRollingAvg()

	// update lagRatio consecutive threshold counts for all topics
	for name, topicMetrics := range s.topicMetrics {
		if topicMetrics.lagRatio > s.metadata.LagRatio {
			if s.topicMetrics[name].partitionsWithLag >= s.metadata.MinPartitionsWithLag {
				// Deault config is 2, dont pointlessly scale if lag is on one partition.
				s.aboveThresholdCount[name]++
				scaleUpTargetMet = true // target is met, may or many not scale up
			} else {
				s.aboveThresholdCount[name] = 0
			}
		} else {
			s.aboveThresholdCount[name] = 0
		}
	}

	// check if we meet MesurementsForScale, and select the most relevant topic for later scale dowwn (largest throughput)
	for name, cnt := range s.aboveThresholdCount {
		if scaleUpOnMultipleTopic {
			// not implemented yet
		} else {
			if cnt >= scaleUpCount {
				scaleUpCount = cnt
				if s.topicMetrics[name].writeRate > topicWrites {
					topicName = name
					topicWrites = s.topicMetrics[name].writeRate
				}
			}
		}
	}

	if scaleUpCount >= s.metadata.MeasurementsForScale {
		// calculate scaleFactor, the Metric multiplier
		if topicName == "" {
			return scaleFactor, scaleUpTargetMet, fmt.Errorf("unexpected error in scale up decision, no topic name")
		}

		tmetrics := s.topicMetrics[topicName]
		s.lastScaleUpTopicName = topicName
		s.lastScaleUpMetrics = &tmetrics
		// This part will skip scaling up if the number of members would become greater than the partition count (3 config options)
		partitions := int64(0)
		switch s.metadata.LimitScaleUp {
		case groupLimit:
			for _, tm := range s.topicMetrics {
				if tm.partitionsTotal > partitions {
					partitions = tm.partitionsTotal
				}
			}
		case topicLimit:
			if s.groupMembersCount >= tmetrics.partitionsTotal {
				// topic that would cause scaling up already has as many members as paritions
				partitions = tmetrics.partitionsTotal
			}
		case noLimit:
			partitions = math.MaxInt64
		default:
			return scaleFactor, scaleUpTargetMet, fmt.Errorf("unexpected value for limitScaleUp found in scale up decision")
		}
		if s.groupMembersCount >= partitions {
			s.logger.V(0).Info(fmt.Sprintf("HPA Metric: not scaling up, group already has one member for each patition (%d)", s.groupMembersCount))
			scaleFactor = 1.0
			s.resetScalingMeasurementsCount()
			return scaleFactor, scaleUpTargetMet, nil
		}

		if tmetrics.readRate > (tmetrics.writeRate * float64(100-s.metadata.WritesToReadTolerance) / 100) {
			// Reads are close to writes or greater
			if tmetrics.readRate > tmetrics.writeRate {
				// check if we should just wait or scale up a notch to catch up faster when reads are higher than writes.
				realLag := tmetrics.lag - tmetrics.residualLag
				lagTimeToNomimal := float64(realLag) / ((tmetrics.readRate * 1000) - (tmetrics.writeRate * 1000))
				if int64(lagTimeToNomimal) > s.metadata.AllowedTimeLagCatchUp {
					scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
					s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Lag catch up time %ds greater than %ds, minimum scale up for topic %s", int64(lagTimeToNomimal), s.metadata.AllowedTimeLagCatchUp, topicName))
				} else {
					scaleFactor = 1.0
					s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Lag XXX catch up time %d lower than %ds, not scaling up topic %s", int64(lagTimeToNomimal), s.metadata.AllowedTimeLagCatchUp, topicName))
				}
			} else {

				// write/s are highve than read/s but just but just within writesToReadTolerance)
				scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: read/s < write/s but within ritesToReadTolerance %d%%,  minimum scale up for topic %s", s.metadata.WritesToReadTolerance, topicName))
			}
		} else {
			// writes can be much higher than reads in situations like initial kafka throughput load is applied suddently
			// or when the consumer group is initially deployed and get min replicas.  A higher metric
			// will accelerate the convergence to correct replicas count

			// internal rates in mgs/ms.   minReadRateToUseForReplicasCount is the minimum read rate
			// necessary to use writes to read ratio.  Too low values can cause excessive scaling up
			if tmetrics.readRate*1000 >= float64(s.metadata.MinReadRateToUseForReplicasCount) {
				scaleFactor = math.Max(tmetrics.writeRate/tmetrics.readRate*s.metadata.WritesToReadRatioDampening, s.metadata.HPAMetricFactorMinimumScaleFactor)
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Using Write/s to Read/s scale factor %.3f for scale up for topic %s", scaleFactor, topicName))
			} else {
				scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Read/s %.3f to low to estimate Write/s to Read/s for scale up for topic %s, minimum scaling", tmetrics.readRate*1000, topicName))
			}
		}
		s.resetScalingMeasurementsCount()
	}

	return scaleFactor, scaleUpTargetMet, nil
}

func (s *kafkaStreamsScaler) getScaleDownDecisionAndFactor() (scaleFactor float64, scaleDownTargetMet bool, err error) {
	scaleFactor = 1.0

	if s.lastScaleUpTopicName == "" || s.lastScaleUpMetrics == nil {
		// no baseline
		return scaleFactor, scaleDownTargetMet, nil
	}
	tmetrics, ok := s.topicMetrics[s.lastScaleUpTopicName]
	if !ok {
		// Somehow, we do not have recent metrics for the topic name that caused last scale up
		s.logger.V(0).Info(fmt.Sprintf("Downscaling check, Group %s has not metrics for topic that caused last scale up: %s", s.metadata.Group, s.lastScaleUpTopicName))
		return scaleFactor, scaleDownTargetMet, nil
	}

	// Basic scale down decision, there is read and write activity on the topic that last caused the scale up and
	// and write throughput is down by configured factor.
	if tmetrics.readRate > 0.0 && tmetrics.writeRate > 0.0 && tmetrics.writeRate < s.lastScaleUpMetrics.writeRate*s.metadata.ScaleDownFactor {
		switch {
		// reads and writes are close, go ahead with scale down
		case withinPercentage(tmetrics.writeRate, tmetrics.readRate, float64(s.metadata.WritesToReadTolerance)):
			s.underThreasholdCount++
			s.logger.V(0).Info(fmt.Sprintf("Scale down condition met (read/s and write/s close), current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, tolerance: %d",
				tmetrics.writeRate*1000, tmetrics.readRate*1000, s.lastScaleUpMetrics.writeRate*1000, s.metadata.WritesToReadTolerance))
		default:
			// TODDO: needs more battle testing.
			s.underThreasholdCount++
			s.logger.V(0).Info(fmt.Sprintf("Scale down condition met (read/s and write/s not close), current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, tolerance: %d",
				tmetrics.writeRate*1000, tmetrics.readRate*1000, s.lastScaleUpMetrics.writeRate*1000, s.metadata.WritesToReadTolerance))
		}
	} else {
		s.underThreasholdCount = 0
		s.logger.V(0).Info(fmt.Sprintf("Scale down condition not met, current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, tolerance: %d",
			tmetrics.writeRate*1000, tmetrics.readRate*1000, s.lastScaleUpMetrics.writeRate*1000, s.metadata.WritesToReadTolerance))
	}

	if s.underThreasholdCount >= s.metadata.MeasurementsForScale {
		scaleDownTargetMet = true
		// HPA metric: desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
		// with the above algo in HPA, if we want to down scale from up to 3 to 2, the metricc must be that low.
		// using HPA policies in the SOPto soften
		scaleFactor = 0.5
	}

	return scaleFactor, scaleDownTargetMet, nil
}

func (s *kafkaStreamsScaler) getAllConsumerGroupMetrics(ctx context.Context) error {
	topicPartitions, groupState, groupMembers, groupHosts, err := s.getTopicPartitions(ctx)
	if err != nil {
		return err
	}
	s.groupState = groupState
	s.groupMembersCount = groupMembers
	s.groupHosts = groupHosts
	s.logger.V(0).Info(fmt.Sprintf("Group: %s, state: %s: number of topics: %d, number of members: %d, number of hosts: %d", s.metadata.Group, groupState, len(topicPartitions), groupMembers, groupHosts))

	consumerOffsets, producerOffsets, err := s.getAllOffsets(ctx, topicPartitions)
	s.logger.V(2).Info(fmt.Sprintf("Group %s, Consumer offsets %v, producer offsets %v", s.metadata.Group, consumerOffsets, producerOffsets))
	if err != nil {
		return err
	}

	// used to record approximate period since last metrics check to calculate per partition write throughout
	// Kafka metrics have variation, no brain surgery precision required.
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for topic, partitionsOffsets := range producerOffsets {
		tmetrics := kafkaTopicMetrics{}
		for partition := range partitionsOffsets {
			pmetrics, err := s.getPartitionMetric(topic, partition, consumerOffsets, producerOffsets, now)
			if err != nil {
				return err
			}
			tmetrics.lag += pmetrics.lag
			tmetrics.residualLag += pmetrics.residualLag
			tmetrics.writeRate += pmetrics.writeRate
			tmetrics.readRate += pmetrics.readRate
			tmetrics.partitionsTotal++
			// lagRatio sum is meaningless, divided by number of paritions below
			if pmetrics.lagRatio > 0 {
				tmetrics.lagRatio += pmetrics.lagRatio
				tmetrics.partitionsWithLag++
			}
		}

		if tmetrics.partitionsWithLag > 0 {
			// Not clear if it ever make sense to set LimitToPartitionsWithLag to false, default is true
			if s.metadata.LimitToPartitionsWithLag {
				tmetrics.lagRatio /= float64(tmetrics.partitionsWithLag)
			} else {
				tmetrics.lagRatio /= float64(tmetrics.partitionsTotal)
			}
		} else {
			tmetrics.lagRatio = 0.0
		}
		tmetrics.period = now - s.lastOffetsTime
		s.topicMetrics[topic] = tmetrics
		// initialize the topic names for holding write/s rolling averages.
		_, ok := s.writesRollingAvg[topic]
		if !ok {
			s.writesRollingAvg[topic] = 0
		}
	}
	// important, update the time we gathered partititon metrics for rates calculations
	s.lastOffetsTime = now

	// log the metrics aggregated for all the topics in the Consumer Group
	for name, topicMetrics := range s.topicMetrics {
		s.logger.V(0).Info(fmt.Sprintf("Group: %s, topic %s, lagRatio: %.3f, lag: %d, partitions/with lag: %d/%d, Write/s %.3f, Read/s %.3f, interval(ms): %d", s.metadata.Group, name, topicMetrics.lagRatio, topicMetrics.lag, topicMetrics.partitionsTotal, topicMetrics.partitionsWithLag, topicMetrics.writeRate*1000, topicMetrics.readRate*1000, topicMetrics.period))
	}
	return nil
}

func (s *kafkaStreamsScaler) getTopicPartitions(ctx context.Context) (map[string][]int, string, int64, int64, error) {
	// Step 1 - get consumer group state and list of topics in the group
	describeGrpReq := &kafka.DescribeGroupsRequest{
		Addr: s.client.Addr,
		GroupIDs: []string{
			s.metadata.Group,
		},
		// TODO: added to make v3 response compabible but does not seem to be helping.
		IncludeAuthorizedOperations: 0,
	}
	// call to the broker
	describeGrp, err := s.client.DescribeGroups(ctx, describeGrpReq)
	if err != nil {
		return nil, "", 0, 0, fmt.Errorf("error describing group: %w", err)
	}
	if len(describeGrp.Groups[0].Members) == 0 {
		return nil, "", 0, 0, fmt.Errorf("no active members in group %s, group-state is %s", s.metadata.Group, describeGrp.Groups[0].GroupState)
	}
	// Requesting a single group, expecting a single response
	groupState := describeGrp.Groups[0].GroupState
	s.logger.V(2).Info(fmt.Sprintf("Consumer Group %s is in state %s", s.metadata.Group, groupState))
	groupMembersCnt := int64(len(describeGrp.Groups[0].Members))
	s.logger.V(2).Info(fmt.Sprintf("Consumer Group %s has %d members", s.metadata.Group, groupMembersCnt))

	// This is sufficient in normal conditions:
	//      describeGrp.Groups[0].Members[0].MemberMetadata.Topics
	// during rebalancing, Members[0] can have no topic.  It may be missing a topic.
	// map for speed to get all the topics from all the members, and make it into an array for kafka-go API
	// also records the number of hosts in the consumer group
	topicsInGroup := make(map[string]struct{})
	hostsInGroup := make(map[string]struct{})
	for _, member := range describeGrp.Groups[0].Members {
		hostsInGroup[member.ClientHost] = struct{}{}
		for _, topic := range member.MemberMetadata.Topics {
			if _, ok := topicsInGroup[topic]; !ok {
				topicsInGroup[topic] = struct{}{}
			}
		}
	}
	hostsCnt := int64(len(hostsInGroup))
	topics := make([]string, 0)
	for name := range topicsInGroup {
		topics = append(topics, name)
	}
	// Calling the Metadata API with empty topics returns all of the paritions for all of the topics
	// on the cluster, lots of data on a large cluster
	if len(topics) == 0 {
		return nil, groupState, groupMembersCnt, hostsCnt, fmt.Errorf("no topic currently assigned to the group: %s in state %s", s.metadata.Group, groupState)
	}
	s.logger.V(2).Info(fmt.Sprintf("Found Topics in Group %s is in state %s", topics, s.metadata.Group))

	// Step 2 - Get the partition numbers for all the topics in the group
	// call to the broker
	clusterMeta, err := s.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   s.client.Addr,
		Topics: topics,
	})
	if err != nil {
		return nil, "", 0, hostsCnt, fmt.Errorf("error getting topics paritions info: %w", err)
	}

	result := make(map[string][]int)
	for _, topic := range clusterMeta.Topics {
		// If the scaler specifies some topic(s) in the consumer groups, consider only those.
		if len(s.metadata.Topic) > 0 && !kedautil.Contains(s.metadata.Topic, topic.Name) {
			continue
		}
		partitions := make([]int, 0)
		for _, partition := range topic.Partitions {
			// If we implement partitionLimitation like in other kafka scalers, that would be here.
			partitions = append(partitions, partition.ID)
		}
		result[topic.Name] = partitions
	}
	return result, groupState, groupMembersCnt, hostsCnt, nil
}

// Fetch last and committed offsets from broker(s), call the 2 APIs required in threds.
type kafkaStreamsConsumerOffsetResult struct {
	consumerOffsets map[string]map[int]int64
	err             error
}
type kafkaStreamsProducerOffsetResult struct {
	producerOffsets map[string]map[int]int64
	err             error
}

func (s *kafkaStreamsScaler) getAllOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, map[string]map[int]int64, error) {
	consumerChan := make(chan kafkaStreamsConsumerOffsetResult, 1)
	go func() {
		consumerOffsets, err := s.getConsumerOffsets(ctx, topicPartitions)
		consumerChan <- kafkaStreamsConsumerOffsetResult{consumerOffsets, err}
	}()

	producerChan := make(chan kafkaStreamsProducerOffsetResult, 1)
	go func() {
		producerOffsets, err := s.getProducerOffsets(ctx, topicPartitions)
		producerChan <- kafkaStreamsProducerOffsetResult{producerOffsets, err}
	}()

	consumerRes := <-consumerChan
	if consumerRes.err != nil {
		return nil, nil, consumerRes.err
	}

	producerRes := <-producerChan
	if producerRes.err != nil {
		return nil, nil, producerRes.err
	}

	return consumerRes.consumerOffsets, producerRes.producerOffsets, nil
}

// Calculate per partition Metrics from the curren offsets and stores in the SO.
func (s *kafkaStreamsScaler) getPartitionMetric(topic string, partitionID int, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64, now int64) (kafkaPartitionMetrics, error) {
	var partitionMetrics kafkaPartitionMetrics

	// Read all the offsets for the topic partition, update previous offsets.
	consumerOffset, previousConsumerOffset, producerOffset, previousProducerOffset := s.getCurrentAndUpdatePreivouOffsets(topic, partitionID, consumerOffsets, producerOffsets)
	if previousConsumerOffset == noPartitionOffset || previousProducerOffset == noPartitionOffset {
		s.logger.V(1).Info("Previous offsets not available (perhaps first check?), cannot compute metrics")
		return partitionMetrics, nil
	}
	if consumerOffset == noPartitionOffset || producerOffset == noPartitionOffset {
		s.logger.V(1).Info("Current offsets could not be read, cannot compute metrics")
		return partitionMetrics, nil
	}
	previousLastOffsettime := s.lastOffetsTime
	period := now - previousLastOffsettime

	if period <= 0 {
		return partitionMetrics, fmt.Errorf("unexpected error calculating period for topic partition %s:%d", topic, partitionID)
	}
	// Write throughput on the partition in messages per milliseconds
	writtenMsg := producerOffset - previousProducerOffset
	if writtenMsg < 0 {
		return partitionMetrics, fmt.Errorf("unexpected error calculating messages/s for topic partition %s:%d", topic, partitionID)
	}
	partitionMetrics.writeRate = float64(writtenMsg) / float64(period)
	// Read throughput on the partition in messages per milliseconds
	readMsg := consumerOffset - previousConsumerOffset
	if readMsg < 0 {
		return partitionMetrics, fmt.Errorf("unexpected error calculating messages/s for topic partition %s:%d", topic, partitionID)
	}
	partitionMetrics.readRate = float64(readMsg) / float64(period)
	// Rates are stored in msg/ms, reported in logs in msg/s, period in ms
	s.logger.V(1).Info(fmt.Sprintf("%.3f writes/s, %.3f reads/s for last %.3f seconds for topic partion %s:%d", partitionMetrics.writeRate*1000, partitionMetrics.readRate*1000, float64(period)/1000, topic, partitionID))

	partitionMetrics.lag = producerOffset - consumerOffset
	// very low write throughput, under 1 msg/s ballpark, can produce high lag ratio, not coutntion those
	if partitionMetrics.writeRate*1000 > s.metadata.MinPartitionWriteThrouput {
		partitionMetrics.residualLag = int64(partitionMetrics.writeRate * float64(s.metadata.CommitInterval) / 2.0)
		partitionMetrics.lagRatio = float64(partitionMetrics.lag) / float64(partitionMetrics.residualLag)
	}
	s.logger.V(1).Info(fmt.Sprintf("lagRatio %.6f based on residualLag %d for topic partion %s:%d", partitionMetrics.lagRatio, partitionMetrics.residualLag, topic, partitionID))
	return partitionMetrics, nil
}

func (s *kafkaStreamsScaler) getConsumerOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, error) {
	response, err := s.client.OffsetFetch(
		ctx,
		&kafka.OffsetFetchRequest{
			GroupID: s.metadata.Group,
			Topics:  topicPartitions,
		},
	)
	if err != nil || response.Error != nil {
		return nil, fmt.Errorf("error listing consumer group offset: %w", err)
	}
	consumerOffset := make(map[string]map[int]int64)
	for topic, partitionsOffset := range response.Topics {
		consumerOffset[topic] = make(map[int]int64)
		for _, partition := range partitionsOffset {
			consumerOffset[topic][partition.Partition] = partition.CommittedOffset
		}
	}

	return consumerOffset, nil
}

// getProducerOffsets returns the latest offsets for the given topic partitions
func (s *kafkaStreamsScaler) getProducerOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, error) {
	// Step 1: build and send OffsetRequest
	offsetRequest := make(map[string][]kafka.OffsetRequest)
	for topic, partitions := range topicPartitions {
		for _, partitionID := range partitions {
			offsetRequest[topic] = append(offsetRequest[topic], kafka.FirstOffsetOf(partitionID), kafka.LastOffsetOf(partitionID))
		}
	}
	res, err := s.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   s.client.Addr,
		Topics: offsetRequest,
	})
	if err != nil {
		return nil, err
	}

	// Step 2: parse response and return
	producerOffsets := make(map[string]map[int]int64)
	for topic, partitionOffset := range res.Topics {
		producerOffsets[topic] = make(map[int]int64)
		for _, partition := range partitionOffset {
			producerOffsets[topic][partition.Partition] = partition.LastOffset
		}
	}

	return producerOffsets, nil
}

// Warning, not idempotent, as the name implies a call to this updates 'previous offset' stored in the scaler
func (s *kafkaStreamsScaler) getCurrentAndUpdatePreivouOffsets(topic string, partitionID int, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (int64, int64, int64, int64) {
	consumerOffset := noPartitionOffset
	previousConsumerOffset := noPartitionOffset
	producerOffset := noPartitionOffset
	previousProducerOffset := noPartitionOffset
	var found bool

	if len(consumerOffsets) != 0 {
		consumerOffset = consumerOffsets[topic][partitionID]
		previousConsumerOffset, found = s.previousConsumerOffsets[topic][partitionID]
		s.logger.V(1).Info(fmt.Sprintf("Got Committed Offset %d, Previous Committed Offset %d for %s:%d, found: %v", consumerOffset, previousConsumerOffset, topic, partitionID, found))
		switch {
		case !found:
			// No record of previous comitted offset, just store current topic and committed offset
			if _, topicFound := s.previousConsumerOffsets[topic]; !topicFound {
				s.previousConsumerOffsets[topic] = map[int]int64{partitionID: consumerOffset}
			} else {
				s.previousConsumerOffsets[topic][partitionID] = consumerOffset
			}
			previousConsumerOffset = noPartitionOffset
		default:
			s.previousConsumerOffsets[topic][partitionID] = consumerOffset
		}
	}

	if len(producerOffsets) != 0 {
		producerOffset = producerOffsets[topic][partitionID]
		previousProducerOffset, found = s.previousLastOffsets[topic][partitionID]
		s.logger.V(1).Info(fmt.Sprintf("Got last Offset %d, Previous last Offset %d for %s:%d, found: %v", producerOffset, previousProducerOffset, topic, partitionID, found))
		switch {
		case !found:
			// No record of previous last offset, store current producer offset
			if _, topicFound := s.previousLastOffsets[topic]; !topicFound {
				s.previousLastOffsets[topic] = map[int]int64{partitionID: producerOffset}
			} else {
				s.previousLastOffsets[topic][partitionID] = producerOffset
			}
			previousProducerOffset = noPartitionOffset
		default:
			s.previousLastOffsets[topic][partitionID] = producerOffset
		}
	}

	s.logger.V(1).Info(fmt.Sprintf("Offsets for group %s topic partition %s:%d, , Last Offset %d, Previous Last Offset %d Committed Offset %d, Previous Committed Offset %d", s.metadata.Group, topic, partitionID, producerOffset, previousProducerOffset, consumerOffset, previousConsumerOffset))

	return consumerOffset, previousConsumerOffset, producerOffset, previousProducerOffset
}

/*

TODO - add auth support, copied from apache-kafka scaler.

func parseApacheKafkaAuthParams(config *scalersconfig.ScalerConfig, meta *apacheKafkaMetadata) error {
	if config.TriggerMetadata["sasl"] != "" && config.AuthParams["sasl"] != "" {
		return errors.New("unable to set `sasl` in both ScaledObject and TriggerAuthentication together")
	}
	if config.TriggerMetadata["tls"] != "" && config.AuthParams["tls"] != "" {
		return errors.New("unable to set `tls` in both ScaledObject and TriggerAuthentication together")
	}
	if meta.SASLType == KafkaSASLTypeMskIam {
		auth, err := awsutils.GetAwsAuthorization(config.TriggerUniqueKey, config.PodIdentity, config.TriggerMetadata, config.AuthParams, config.ResolvedEnv)
		if err != nil {
			return err
		}
		meta.AWSAuthorization = auth
	}
	return nil
}

func parseApacheKafkaMetadata(config *scalersconfig.ScalerConfig) (apacheKafkaMetadata, error) {
	meta := apacheKafkaMetadata{triggerIndex: config.TriggerIndex}
	if err := config.TypedConfig(&meta); err != nil {
		return meta, fmt.Errorf("error parsing kafka metadata: %w", err)
	}

	if err := parseApacheKafkaAuthParams(config, &meta); err != nil {
		return meta, err
	}

	return meta, nil
}
*/
