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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asecurityteam/rolling"
	"github.com/go-logr/logr"
	awsutils "github.com/kedacore/keda/v2/pkg/scalers/aws"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	"gonum.org/v1/gonum/stat"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

/*
 To emit the single metric in getMetricsAndActiviy(), this scaler upses kafka metrics from the brokers.
 All those internal metrics rely on a time interval, for this reason and due to the 'lively'
 nature of kakfka consumer offsets metrics, thought the scaler will adapt to SO cofiguration, it will produce best results
 with a polling interval of a minute or more with HPA defaul config of --horizontal-pod-autoscaler-sync-period 15 seconds.

 Scaler logic assumes metricType: Value and useCachedMetrics as below, it makes scaling decisions at the consumer group
 level, metricType: Average does not apply to this scaler

spec:
  pollingInterval: 60
    triggers:
  - type: kafka-streams
    metricType: Value
    useCachedMetrics: true

Over time and with encountering different real life situations, the scaler has been made more and more complex to adapt
and behave well in a variety of situations.  This scaler has exceeded the complexity level that can be reasonably
used as an internal scaler in KEDA.  This code is waiting to be migrated to an external scaler
*/

// Kafka metrics evaluated for each topic partition in the consumer group
type kafkaPartitionMetrics struct {
	writeRate, readRate float64 // Rates in msg/millisecond
	lag                 int64   // standard Kafka lag, in number of messages
	residualLag         int64   // measure of avegrage expected lag in a consumer group that consumes messages close to 'as they are produced', in number of messages
}

// Kafka metrics evaluated for each topic in the consumer group
type kafkaTopicMetrics struct {
	WriteRate         float64 // Rates in msg/millisecond
	ReadRate          float64
	Lag               int64 // in number of messages
	ResidualLag       int64
	LagRatio          float64 // no unit, lag/residualLag
	Period            int64   // number of milliseconds over which above rates are calculated,  with useCachedMetrics: true, it should be close to pollingInterval
	PartitionsWithLag int64   // # topic partitions with a measurable lag
	PartitionsTotal   int64   // # total partitions in the topic
}

// metrics and state for each Scale Object
type kafkaStreamsScaler struct {
	metricType v2.MetricTargetType
	metadata   *kafkaStreamsMetadata // scaler config from coarse validation of user input + default values
	client     *kafka.Client         // kafka-go client
	logger     logr.Logger
	// Scaler state
	// 'previous' values are from last polling interval
	previousConsumerOffsets   map[string]map[int]int64        // committed offsets for all the topics and topic parttions in last poll
	previousLastOffsets       map[string]map[int]int64        // last offsets for all the topics and topic parttions in last poll
	lastOffetsTime            int64                           // timestamp where all those offsets were updated
	topicMetrics              map[string]kafkaTopicMetrics    // Calculated metrics for each topic used for scaling decisions
	writesAggWindow           map[string]*rolling.PointPolicy // rolling window of write/s for each topic
	writesRollingAvg          map[string]float64              // Approximate write/s rolling avg
	writesRollingStdDev       map[string]float64              // Approximate write/s rolling standard deviation
	aboveThresholdCount       map[string]int64                // Consecutive polling periods where lagRatio is met for 'MeasurementsForScale' for scale up
	underThreasholdCount      int64                           // Consecutive polling periods where lagRatio is met for 'MeasurementsForScale' for scale down
	groupState                string                          // last poll consumer group state
	groupMembersCount         int64                           // Number of members in the consumer group
	previousGroupMembersCount int64                           // Number of members in the consumer group in previous poll
	groupHosts                int64                           // Number of hosts in the consumer group
	lastScaleUpTopicName      string                          // Store the name of the last topic for which metrics caused a scale up
	lastScaleUpMetrics        *kafkaTopicMetrics              // Store metrics of the last topic that casued scale up
	pollingCount              int64                           // Times the getMetricsAndActivity() API was called by keda.
	pollingStableCount        int64                           // Times the getMetricsAndActivity() API was called by keda and a metric could be calculated
	underWriteThresholdDownSc int64                           // Timetamp of when we first see write threshold coming too low,too fast. Used for the period to pause scale down
}

/*
 This scaler keeps track internally of the number of polling intervals where metric is above target with MeasurementsForScale
 It would be nice to use HPA policies, but 'periodSeconds' and 'stabilizationWindowSeconds' semantic is different, rolling
 maximum is a different behavior thatn emitting a single metric that will cause scale up/down'.  Better decision can
 be made in the scaler rater than emitting a metric at ever polloing interval and counting on policies and rolling max for final decisions.
 This scaler will produce best results with settiongs like this:
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
              value: 10
              periodSeconds: 60
            - type: Pods
              value: 1
              periodSeconds: 60
	    scaleDown:
          stabilizationWindowSeconds: 0
          selectPolicy: Max
          policies:
            - type: Pods
              value: 1
              periodSeconds: 60
            - type: Percent
              value: 10
              periodSeconds: 60

Or make downscaleing more conservative by removingthe Pecent policy from scale down.
*/

// see defaultLimitScaleUp
type LimitScaleUp int

const (
	noLimit LimitScaleUp = iota
	topicLimit
	groupLimit
)

const (
	noPartitionOffset = int64(-1)
	// default Values for trigger parameters
	defaultLagRatio                          = 3.0          // no unit
	defaultCommitInterval                    = 30000        // milliseconds, default commit interval in Java Kafka streaming library.
	defaultMinPartitionWriteThrouput         = 0.5          // in msg/secs.  lagRatio will not be calculated when throuhput is lower than this value
	defaultMeasurementsForScaleUp            = 3            // number of polling intervals where conditions for scale are met before scaling up
	defaultMeasurementsForScaleDown          = 10           // number of polling intervals where conditions for scale are met before scaling down
	defaultMeasurementsForScaleInitial       = 3            // number of polling intervals between scale downs at initial installation
	defaultRollingMeasurements               = 15           // number of polling intervals for rolling average of writes/s and also for stddev calculation
	defaultScaleDownFactor                   = 0.60         // How much write rates to topic have to come down from last scale up to initiate downscale,
	defaultLimitToPartitionsWithLag          = true         // when true, average lagratio at the topic level ignoring partitions with no writes.
	defaultAllowedTimeLagCatchUp             = 600          // if consumerGroup is estimated to catchup lag under that time in seconds, do not scale up
	defaultWritesToReadTolerance             = 20           // Tolerance to decide if reads and writes are 'close' one another, in percentage
	defaultWritesToReadRatioDampening        = 0.66         // when calculating an HPA metric using the writes to read ratio, use a damping factor to avoid replicas overshoot
	defaultMinReadRateToUseForReplicasCount  = 10           // Do not use writes to consumer read ratio to estimate HPA metric if topic read rate is lower in msg/s
	defaultHPAMetricFactorMinimumScaleFactor = 1.11         // Target * 1.11 is just above HPA globally-configurable tolerance, 0.1 by default.
	defaultLimitScaleUp                      = groupLimit   // Limit scaling if group Members would exceed partitions: "group" -> topic with max partitions, "topic" -> topic causing scaling up, "none"
	defaultMinPartitionsWithLag              = 2            // Do not scale unless the lag is on at least that many parition.
	defaultIgnoreTopicsMatching              = "-KSTREAM"   // Coma separated list of strings, topics names containing this substring will not be used for scaling.
	defaultResetOnHostCount                  = false        // with a short polling interval, we want to leave time for host count to stabilize, detrimental with long
	defaultMinMembersScaleDownFloor          = 2            // Scale down will stop once the consumer group members is down to this value
	defaultScalingPaused                     = false        // Set to true for calculating all metrics and emit the logs, but pause scaling
	defaultMinWritesForScaleDown             = 20           // Pause scale down if Write throughput suddently falls un this value percentage (likely temporary)
	defaultScalePauseTime                    = 60 * 60 * 18 // Time in seconds to pause scale down when  Write throughput suddently falls
	DefaultResetMeticsOnStartup              = false        // When true, scaler will not use saved metrics from compacted topic on startup

	// Not configuratble (yet) default parameters for scaling decision.
	scaleUpOnMultipleTopic = false // When true (not implemented!), scale on any combination of topic meeting threshold after MeasurementsForScale
)

type kafkaStreamsMetadata struct {
	// Madatory Metadata
	BootstrapServers []string
	Group            string
	// Optional Metadata with no default values
	Topics []string
	// Optional Metadata with default values
	LagRatio                          float64
	CommitInterval                    int64
	MinPartitionWriteThrouput         float64
	MeasurementsForScaleUp            int64
	MeasurementsForScaleDown          int64
	MeasurementsForScaleInitial       int64
	RollingMeasurements               int64
	ScaleDownFactor                   float64
	LimitToPartitionsWithLag          bool
	AllowedTimeLagCatchUp             int64
	WritesToReadTolerance             int64
	WritesToReadRatioDampening        float64
	MinReadRateToUseForReplicasCount  int64
	HPAMetricFactorMinimumScaleFactor float64
	LimitScaleUp                      LimitScaleUp
	MinPartitionsWithLag              int64
	IgnoreTopicsMatching              []string
	ResetOnHostCount                  bool
	MinMembersScaleDownFloor          int64
	ScalingPaused                     bool
	MinWritesForScaleDown             int64
	ScalePauseTime                    int64
	ResetMeticsOnStartup              bool

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

// Metrics are updated on a scale up event and form a base like for write/s rate.
// Scaler uses a compacted topic for persistent storage of learned topic rates for downscaling
// Metrics are read at start up to initialize the scaler

const kedaKafaStreamsTopic = "keda-kafka-streams-rates"

// There is a single producer instance shared with all the SOs
type kedaKafkaProducer struct {
	mu       sync.Mutex
	producer *kafka.Writer
}

var kedaProducer kedaKafkaProducer

// Only the first SO to start will read the compacted topic once and all SO will
// initialize their state from global kafkaStreamsSavedMetrics
type persistentTopicMetric struct {
	TopicName   string
	TopicMetric kafkaTopicMetrics
}
type kafkaStreamsPersistentMetrics struct {
	mu           sync.Mutex
	initialized  bool
	savedMetrics map[string]persistentTopicMetric // key is Group name
}

var kafkaStreamsSavedMetrics kafkaStreamsPersistentMetrics

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
	if val, ok := config.TriggerMetadata["topics"]; ok {
		names := strings.Split(val, ",")
		meta.Topics = append(meta.Topics, names...)
	}
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
		if err != nil || minThroughput < 0.0 {
			return nil, fmt.Errorf("minPartitionWriteThrouput must be a float in bytes/second greater or equal than 0")
		}
		meta.MinPartitionWriteThrouput = minThroughput
	} else {
		meta.MinPartitionWriteThrouput = defaultMinPartitionWriteThrouput
	}
	if val, ok := config.TriggerMetadata["measurementsForScaleUp"]; ok {
		measurements, err := strconv.ParseInt(val, 10, 64)
		if err != nil || measurements <= 0 {
			return nil, fmt.Errorf("measurementsForScaleUp must be a inteter number greater than 0")
		}
		meta.MeasurementsForScaleUp = measurements
	} else {
		meta.MeasurementsForScaleUp = defaultMeasurementsForScaleUp
	}
	if val, ok := config.TriggerMetadata["measurementsForScaleDown"]; ok {
		measurements, err := strconv.ParseInt(val, 10, 64)
		if err != nil || measurements <= 0 {
			return nil, fmt.Errorf("measurementsForScaleDown must be a inteter number greater than 0")
		}
		meta.MeasurementsForScaleDown = measurements
	} else {
		meta.MeasurementsForScaleDown = defaultMeasurementsForScaleDown
	}
	if val, ok := config.TriggerMetadata["measurementsForScaleInitial"]; ok {
		measurements, err := strconv.ParseInt(val, 10, 64)
		if err != nil || measurements <= 0 {
			return nil, fmt.Errorf("measurementsForScaleInitial must be a inteter number greater than 0")
		}
		meta.MeasurementsForScaleInitial = measurements
	} else {
		meta.MeasurementsForScaleInitial = defaultMeasurementsForScaleInitial
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
	if val, ok := config.TriggerMetadata["rollingMeasurements"]; ok {
		window, err := strconv.ParseInt(val, 10, 64)
		if err != nil || window < 3 || window > 60 {
			return nil, fmt.Errorf("rollingMeasurements must be a int number between 3 and 60")
		}
		meta.RollingMeasurements = window
	} else {
		meta.RollingMeasurements = defaultRollingMeasurements
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
	if val, ok := config.TriggerMetadata["ignoreTopicsMatching"]; ok {
		names := strings.Split(val, ",")
		meta.IgnoreTopicsMatching = append(meta.IgnoreTopicsMatching, names...)
	} else {
		meta.IgnoreTopicsMatching = append(meta.IgnoreTopicsMatching, defaultIgnoreTopicsMatching)
	}
	if val, ok := config.TriggerMetadata["resetOnHostCount"]; ok {
		reset, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("resetOnHostCount must be \"true\" or \"false\"")
		}
		meta.ResetOnHostCount = reset
	} else {
		meta.ResetOnHostCount = defaultResetOnHostCount
	}
	if val, ok := config.TriggerMetadata["minMembersScaleDownFloor"]; ok {
		min, err := strconv.ParseInt(val, 10, 64)
		if err != nil || min < 1 {
			return nil, fmt.Errorf("minMembersScaleDownFloor must be a inteter greater or equal than 1")
		}
		meta.MinMembersScaleDownFloor = min
	} else {
		meta.MinMembersScaleDownFloor = defaultMinMembersScaleDownFloor
	}
	if val, ok := config.TriggerMetadata["scalingPaused"]; ok {
		paused, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("scalingPaused must be \"true\" or \"false\"")
		}
		meta.ScalingPaused = paused
	} else {
		meta.ScalingPaused = defaultScalingPaused
	}
	if val, ok := config.TriggerMetadata["minWritesForScaleDown"]; ok {
		mw, err := strconv.ParseInt(val, 10, 64)
		if err != nil || mw < 0 || mw > 100 {
			return nil, fmt.Errorf("minWritesForScaleDown must be a inteter percentage value between 0 and 100")
		}
		meta.MinWritesForScaleDown = mw
	} else {
		meta.MinWritesForScaleDown = defaultMinWritesForScaleDown
	}
	if val, ok := config.TriggerMetadata["scalePauseTime"]; ok {
		val, err := strconv.ParseInt(val, 10, 64)
		if err != nil || val < 0 {
			return nil, fmt.Errorf("scalePauseTime must be a inteter value in seconds greater than 0")
		}
		meta.ScalePauseTime = val
	} else {
		meta.ScalePauseTime = defaultScalePauseTime
	}
	if val, ok := config.TriggerMetadata["resetMeticsOnStartup"]; ok {
		reset, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("ResetMeticsOnStartup must be \"true\" or \"false\"")
		}
		meta.ResetMeticsOnStartup = reset
	} else {
		meta.ResetMeticsOnStartup = DefaultResetMeticsOnStartup
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
	logger.V(0).Info("NewKafkaStreamScaler: Initializing scaler")

	// kafka-go, each scaler has it's own client, producer is shared with all scalers.
	client, err := getKafkaGoClient(ctx, *meta, logger)
	if err != nil {
		return nil, err
	}
	// Created the compacted topic if it does not exist yet
	err = createProducerTopic(client)
	if err != nil {
		return nil, err
	}
	kedaProducer.createProducer(meta.BootstrapServers)

	// Metrics loaded from compacted topic only once for all SO by the first SO being created.
	kafkaStreamsSavedMetrics.ReadCompactedTopic(meta.BootstrapServers, logger)
	m, ok := kafkaStreamsSavedMetrics.savedMetrics[meta.Group]
	lst := "Not Set"
	var lsm *kafkaTopicMetrics
	if ok && meta.ResetMeticsOnStartup == false {
		logger.V(1).Info(fmt.Sprintf("Compacted Topic - Read Group:%s, topic: %s, write/ms: %f",
			meta.Group, m.TopicName, m.TopicMetric.WriteRate))
		lst = m.TopicName
		lsm = &m.TopicMetric
	} else {
		// Any SO/consumer group that never scaled up will not have a saved metric.
		// This should be the case for consumer groups suck at "minMembersScaleDownFloor", default 2
		logger.V(1).Info(fmt.Sprintf("Compacted Topic - Group %s not found", meta.Group))
	}

	previousConsumerOffsets := make(map[string]map[int]int64)
	previousLastOffsets := make(map[string]map[int]int64)
	topicMetrics := make(map[string]kafkaTopicMetrics)
	writesAggWindow := make(map[string]*rolling.PointPolicy)
	writesRollingAvg := make(map[string]float64)
	writesRollingStdDev := make(map[string]float64)
	aboveThresholdCount := make(map[string]int64)

	return &kafkaStreamsScaler{
		client:                  client,
		metricType:              metricType,
		metadata:                meta,
		logger:                  logger,
		previousConsumerOffsets: previousConsumerOffsets,
		previousLastOffsets:     previousLastOffsets,
		topicMetrics:            topicMetrics,
		writesAggWindow:         writesAggWindow,
		writesRollingAvg:        writesRollingAvg,
		writesRollingStdDev:     writesRollingStdDev,
		aboveThresholdCount:     aboveThresholdCount,
		lastScaleUpTopicName:    lst,
		lastScaleUpMetrics:      lsm,
	}, nil
}

// Scaler Interface -- GetMetricsAndActivity()
func (s *kafkaStreamsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	s.logger.V(1).Info("GetMetricsAndActivity")
	metricVal, err := s.getMetricForHPA(ctx)
	if err != nil {
		// log the reason of the failed metric calculation
		s.logger.V(0).Info(fmt.Sprintf("HPA final, Metric = TARGET, no mesurement due to %s", err))
		re, ok := err.(*consumerGroupError)
		if ok {
			if re.PermanentError() {
				// returning error caused the scaler to be re-created, not good.
				// log the error, dont return it to keda, and returning activity = False shows on the SO as Active = False.
				return []external_metrics.ExternalMetricValue{}, false, nil
			}
		}
	}

	metric := GenerateMetricInMili(metricName, metricVal)
	return []external_metrics.ExternalMetricValue{metric}, true, nil
}

// Scaler Interface -- GetMetricSpecForScaling()
// Cosmetic issue.  Consider using a different TARGET?  TARGET is set at lagRatio threshold, however although LagRatio is a key element for scaling up,
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

// Scaler Interface: Close()
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

// Update consumer group topic & partitions metrics, make scaling decsion, calculate the SO metric for HPA
func (s *kafkaStreamsScaler) getMetricForHPA(ctx context.Context) (float64, error) {
	s.pollingCount++ // internal stat, not used for scaling.

	hpaMetric := s.metadata.LagRatio // initialized to TARGET
	err := s.getAllConsumerGroupMetrics(ctx)
	if err != nil {
		s.resetScalingMeasurementsCount()
		return hpaMetric, err
	}

	err = s.isConsumerGroupStatble()
	if err != nil {
		// consumer group is not stable, restart threshold counts.
		s.resetScalingMeasurementsCount()
		return hpaMetric, err
	}

	factor, scaleUpTargetMet, err := s.getScaleUpDecisionAndFactor()
	if err != nil {
		s.resetScalingMeasurementsCount()
		s.logger.V(0).Info(err.Error())
		return hpaMetric, err
	}

	scaleDownTargetMet := false

	if !scaleUpTargetMet {
		factor, scaleDownTargetMet, err = s.getScaleDownDecisionAndFactor()
		if err != nil {
			s.resetScalingMeasurementsCount()
			s.logger.V(0).Info(err.Error())
			return hpaMetric, err
		}
	}

	// Pick most relevant topic for logging/debugging only.
	// TODO: keep this?   lotss of code for creating one log entry...
	topicInfoForLog := ""
	switch {
	case scaleUpTargetMet:
		a := int64(0)
		ratio := 0.0
		for name, cnt := range s.aboveThresholdCount {
			r := s.topicMetrics[name].LagRatio
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
		if s.lastScaleUpTopicName != "Not Set" {
			topicInfoForLog = s.lastScaleUpTopicName
			break
		}
		fallthrough
	default:
		ratio := 0.0
		for name, topicMetrics := range s.topicMetrics {
			if topicMetrics.LagRatio > ratio {
				ratio = topicMetrics.LagRatio
				topicInfoForLog = name
			}
		}
	}

	met := s.topicMetrics[topicInfoForLog]
	action := "Scaling: no"
	switch {
	case s.metadata.ScalingPaused == true:
		action = "Scaling: DISABLED: "
	case factor > 1.0:
		action = "Scaling: up: "
	case factor < 1.0:
		action = "Scaling: down: "
	}

	s.logger.V(0).Info(fmt.Sprintf("%s, Final Metric: %.3f, Group state:%s, lag ratio: %.3f, counts up/down: %d/%d, lag: %d, residual lag: %d, write/s: %.1f, read/s: %.1f, write/s rolling avg: %.1f, write/s stdev: %.1f, CV: %.1f%%, group: %s on topic: %s",
		action, hpaMetric*factor, s.groupState, met.LagRatio, s.aboveThresholdCount[topicInfoForLog], s.underThreasholdCount, met.Lag, met.ResidualLag, met.WriteRate*1000, met.ReadRate*1000, s.writesRollingAvg[topicInfoForLog]*1000,
		s.writesRollingStdDev[topicInfoForLog]*1000, s.writesRollingStdDev[topicInfoForLog]/s.writesRollingAvg[topicInfoForLog]*100, s.metadata.Group, topicInfoForLog))
	if s.lastScaleUpMetrics != nil {
		s.logger.V(1).Info(fmt.Sprintf("Final Metric: last scale up topic: %s, write/s: %f", s.lastScaleUpTopicName, s.lastScaleUpMetrics.WriteRate*1000))
	}

	if s.metadata.ScalingPaused {
		// return metric = TARGET in this mode.
		factor = 1.0
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
	s.underThreasholdCount = 0
}

// Update topics write/s metrics average over N periods, with N windown size = RollingMeasurements
func (s *kafkaStreamsScaler) updateRollingAvg() {
	if s.pollingStableCount >= s.metadata.RollingMeasurements {
		for name := range s.writesRollingAvg {
			s.writesRollingAvg[name] = s.writesAggWindow[name].Reduce(rolling.Avg)
			s.writesRollingStdDev[name] = s.writesAggWindow[name].Reduce(func(w rolling.Window) float64 {
				// Window is a [][]float64, not ideal for stddev calculation, flatten it first
				var flatten []float64
				for _, row := range w {
					flatten = append(flatten, row...)
				}
				s.logger.V(0).Info(fmt.Sprintf("Recorded rolling write/s values for toic: %s, %v", name, flatten))
				return stat.StdDev(flatten, nil)
			})
			s.logger.V(2).Info(fmt.Sprintf("Rolling metrics for topics: %s, Polls:%d, Roll avg: %f, Roll StdDev: %f ", name, s.pollingStableCount, s.writesRollingAvg[name], s.writesRollingStdDev[name]))
		}
	}
	s.pollingStableCount++
}

// Check if consumer group is not stable or members number has changed since last poll
func (s *kafkaStreamsScaler) isConsumerGroupStatble() (err error) {
	// No scaling action unless the consumer group is 'Stable', reset all counts and re-start measuring when stable.
	if s.groupState != "Stable" {
		return fmt.Errorf("reset measurements counts for group: %s in state %s", s.metadata.Group, s.groupState)
	}
	// No scaling action if the number of hosts in the consumer group changed since last polling interval
	lastHostCount := s.previousGroupMembersCount
	s.previousGroupMembersCount = s.groupMembersCount
	if s.metadata.ResetOnHostCount && s.groupMembersCount != lastHostCount {
		return fmt.Errorf("reset measurements counts for group: %s hosts count changed from %d to %d", s.metadata.Group, lastHostCount, s.groupMembersCount)
	}
	return nil
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
	var tmetrics kafkaTopicMetrics

	s.updateRollingAvg()

	// update lagRatio consecutive threshold counts for all topics
	for name, topicMetrics := range s.topicMetrics {
		if topicMetrics.LagRatio > s.metadata.LagRatio {
			if s.topicMetrics[name].PartitionsWithLag >= s.metadata.MinPartitionsWithLag {
				// Default config is 2, dont pointlessly scale if lag is on one partition.
				s.aboveThresholdCount[name]++
				scaleUpTargetMet = true // target is met, may or many not scale up
				s.underThreasholdCount = 0
			} else {
				s.aboveThresholdCount[name] = 0
			}
		} else {
			s.aboveThresholdCount[name] = 0
		}
	}

	// check if we meet MesurementsForScale  consecutive thresholds, and select the most relevant topic for later scale dowwn (largest throughput)
	for name, cnt := range s.aboveThresholdCount {
		if scaleUpOnMultipleTopic {
			// not implemented yet
		} else {
			if cnt >= scaleUpCount {
				scaleUpCount = cnt
				if s.topicMetrics[name].WriteRate > topicWrites {
					topicName = name
					topicWrites = s.topicMetrics[name].WriteRate
				}
			}
		}
	}

	if scaleUpCount >= s.metadata.MeasurementsForScaleUp {
		// calculate scaleFactor, the Metric multiplier
		if topicName == "" {
			return scaleFactor, scaleUpTargetMet, fmt.Errorf("unexpected error in scale up decision, no topic name")
		}

		tmetrics = s.topicMetrics[topicName]
		s.lastScaleUpTopicName = topicName
		s.lastScaleUpMetrics = &tmetrics

		// This part will skip scaling up if the number of members would become greater than the partition count (3 config options)
		partitions := int64(0)
		switch s.metadata.LimitScaleUp {
		case groupLimit:
			for _, tm := range s.topicMetrics {
				if tm.PartitionsTotal > partitions {
					partitions = tm.PartitionsTotal
				}
			}
		case topicLimit:
			if s.groupMembersCount >= tmetrics.PartitionsTotal {
				// topic that would cause scaling up already has as many members as paritions
				partitions = tmetrics.PartitionsTotal
			}
		case noLimit:
			partitions = math.MaxInt64
		default:
			return scaleFactor, scaleUpTargetMet, fmt.Errorf("unexpected value for limitScaleUp found in scale up decision")
		}

		if s.groupMembersCount >= partitions {
			scaleFactor = 1.0
			s.resetScalingMeasurementsCount()
			return scaleFactor, scaleUpTargetMet, fmt.Errorf("HPA Metric: not scaling up, group already has one member for each patition (%d)", s.groupMembersCount)
		}

		if tmetrics.ReadRate > (tmetrics.WriteRate * float64(100-s.metadata.WritesToReadTolerance) / 100) {
			// Reads are close to writes or greater
			if tmetrics.ReadRate > tmetrics.WriteRate && tmetrics.Lag > tmetrics.ResidualLag {
				// check if we should just wait or scale up a notch to catch up faster when reads are higher than writes.
				realLag := tmetrics.Lag - tmetrics.ResidualLag
				lagTimeToNomimal := float64(realLag) / ((tmetrics.ReadRate * 1000) - (tmetrics.WriteRate * 1000))
				if int64(lagTimeToNomimal) > s.metadata.AllowedTimeLagCatchUp {
					scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
					s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Lag catch up time %ds greater than %ds, minimum scale up for topic %s", int64(lagTimeToNomimal), s.metadata.AllowedTimeLagCatchUp, topicName))
				} else {
					scaleFactor = 1.0
					s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Lag catch up time %d lower than %ds, not scaling up topic %s", int64(lagTimeToNomimal), s.metadata.AllowedTimeLagCatchUp, topicName))
				}
			} else {
				// write/s are higher than read/s but just but just within writesToReadTolerance)
				scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: read/s < write/s but within writesToReadTolerance %d%%,  minimum scale up for topic %s", s.metadata.WritesToReadTolerance, topicName))
			}
		} else {
			// writes can be much higher than reads in situations like initial kafka throughput load is applied suddently
			// or when the consumer group is initially deployed and get min replicas.  A higher metric
			// will accelerate the convergence to correct replicas count

			// internal rates in mgs/ms.   minReadRateToUseForReplicasCount is the minimum read rate
			// necessary to use writes to read ratio.  Too low values can cause excessive scaling up
			if tmetrics.ReadRate*1000 >= float64(s.metadata.MinReadRateToUseForReplicasCount) {
				scaleFactor = math.Max(tmetrics.WriteRate/tmetrics.ReadRate*s.metadata.WritesToReadRatioDampening, s.metadata.HPAMetricFactorMinimumScaleFactor)
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Using Write/s to Read/s scale factor %.3f for scale up for topic %s", scaleFactor, topicName))
			} else {
				scaleFactor = s.metadata.HPAMetricFactorMinimumScaleFactor
				s.logger.V(0).Info(fmt.Sprintf("HPA Metric: Read/s %.3f to low to estimate Write/s to Read/s for scale up for topic %s, minimum scaling", tmetrics.ReadRate*1000, topicName))
			}
		}
		s.resetScalingMeasurementsCount()
	}

	// save metrics on compacted topic when we are really scaling up.
	if scaleFactor > 1.0 && s.metadata.ScalingPaused == false {
		kedaProducer.publishConsumerGroupMetrics(s, topicName, &tmetrics)
	}
	return scaleFactor, scaleUpTargetMet, nil
}

func (s *kafkaStreamsScaler) getScaleDownDecisionAndFactor() (scaleFactor float64, scaleDownTargetMet bool, err error) {
	scaleFactor = 1.0
	writes := 0.0
	nIntervals := s.metadata.MeasurementsForScaleDown
	if s.lastScaleUpTopicName == "" || s.lastScaleUpMetrics == nil {
		// no baseline, let's scale down to unless we reached mimimum consumer group memebers
		if s.groupHosts > s.metadata.MinMembersScaleDownFloor {
			s.logger.V(0).Info(fmt.Sprintf("Downscaling check, Group %s has no saved metrics, will scale down after %d consecutive checks", s.metadata.Group, s.metadata.MeasurementsForScaleInitial))
			s.underThreasholdCount++
			scaleDownTargetMet = true
			nIntervals = s.metadata.MeasurementsForScaleInitial
		}
	} else {
		tmetrics, ok := s.topicMetrics[s.lastScaleUpTopicName]
		if !ok {
			// Somehow, we do not have recent metrics for the topic name we saved that caused last scale up
			// ot supposed to happen, right behaviour?
			return scaleFactor, false, fmt.Errorf("unexpected scaler state, topic missing metrics: %s", s.lastScaleUpTopicName)
		}
		writes = tmetrics.WriteRate
		if tmetrics.ReadRate > 0.0 && tmetrics.WriteRate > 0.0 && tmetrics.WriteRate < s.lastScaleUpMetrics.WriteRate*s.metadata.ScaleDownFactor {
			// Basic scale down decision, there is read and write activity on the topic that last caused the scale up and
			// and write throughput is down by configured factor.
			s.underThreasholdCount++
			scaleDownTargetMet = true
			if withinPercentage(tmetrics.WriteRate, tmetrics.ReadRate, float64(s.metadata.WritesToReadTolerance)) {
				// reads and writes are close, go ahead with scale down - may implement different behavior in the future.
				s.logger.V(0).Info(fmt.Sprintf("Scale down condition met (read/s and write/s close), current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, r/w tolerance: %d%%, scaleDownFactor: %f",
					tmetrics.WriteRate*1000, tmetrics.ReadRate*1000, s.lastScaleUpMetrics.WriteRate*1000, s.metadata.WritesToReadTolerance, s.metadata.ScaleDownFactor))
			} else {
				s.logger.V(0).Info(fmt.Sprintf("Scale down condition met (read/s and write/s not close), current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, r/w tolerance: %d%%, scaleDownFactor: %f",
					tmetrics.WriteRate*1000, tmetrics.ReadRate*1000, s.lastScaleUpMetrics.WriteRate*1000, s.metadata.WritesToReadTolerance, s.metadata.ScaleDownFactor))
			}
		} else {
			s.underWriteThresholdDownSc = 0
			s.underThreasholdCount = 0
			s.logger.V(0).Info(fmt.Sprintf("Scale down condition not met, current writes/s %.3f, read/s %.3f, registered peak writes/s %.3f, r/w tolerance: %d%% on topic %s, scaleDownFatcor: %f, minWritesForScaleDown:%d%%",
				tmetrics.WriteRate*1000, tmetrics.ReadRate*1000, s.lastScaleUpMetrics.WriteRate*1000, s.metadata.WritesToReadTolerance, s.lastScaleUpTopicName, s.metadata.ScaleDownFactor, s.metadata.MinWritesForScaleDown))
		}
	}

	if s.underThreasholdCount >= nIntervals {
		if writes > 0 && writes < s.lastScaleUpMetrics.WriteRate*float64(s.metadata.MinWritesForScaleDown)/100.0 {
			// Write rates fell too low too fast, engage down scaling pause
			// In initial deployment, with no recored writes, writes will be 0.
			now := time.Now().Unix() // In seconds
			if s.underWriteThresholdDownSc == 0 {
				s.underWriteThresholdDownSc = now
			} else {
				s.logger.V(2).Info(fmt.Sprintf("DEBUG now:%d, s.underWriteThresholdDownSc: %d", now, s.underWriteThresholdDownSc))
				if now-s.underWriteThresholdDownSc > s.metadata.ScalePauseTime {
					s.logger.V(0).Info("Scale down condition was met, exceeded the pause period forlow throughput, scaling down!")

					// pause period is over
					s.underWriteThresholdDownSc = 0
					scaleFactor = 0.5
				} else {
					ftime := time.Unix(s.underWriteThresholdDownSc, 0)
					s.logger.V(0).Info(fmt.Sprintf("Scale down condition was met but writes under minimum threshold (temporary condition?) current writes/s %.3f,registered peak writes/s %.3f, minWritesForScaleDown:%d%%, started at %s",
						writes, s.lastScaleUpMetrics.WriteRate*1000, s.metadata.MinWritesForScaleDown, ftime.Format(time.RFC3339)))
				}
			}
		} else {
			s.underWriteThresholdDownSc = 0
			s.resetScalingMeasurementsCount()
			// HPA metric: desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
			// with the above algo in HPA, if we want to down scale from up to 3 to 2, the metricc must be that low.
			// using HPA policies in the SOPto soften
			scaleFactor = 0.5
		}
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
			tmetrics.Lag += pmetrics.lag
			tmetrics.ResidualLag += pmetrics.residualLag
			tmetrics.WriteRate += pmetrics.writeRate
			tmetrics.ReadRate += pmetrics.readRate
			tmetrics.PartitionsTotal++
			if pmetrics.lag > 0 {
				tmetrics.PartitionsWithLag++
			}
		}
		if tmetrics.ResidualLag > 0 {
			// Topics Very small throughput can produce large LagRatio, ignore
			if s.metadata.LimitToPartitionsWithLag {
				if tmetrics.WriteRate*1000/float64(tmetrics.PartitionsWithLag) > s.metadata.MinPartitionWriteThrouput {
					tmetrics.LagRatio = float64(tmetrics.Lag) / float64(tmetrics.ResidualLag)
				}
			} else {
				if tmetrics.WriteRate*1000/float64(tmetrics.PartitionsTotal) > s.metadata.MinPartitionWriteThrouput {
					tmetrics.LagRatio = float64(tmetrics.Lag) / float64(tmetrics.ResidualLag)
				}
			}
		}
		tmetrics.Period = now - s.lastOffetsTime
		s.topicMetrics[topic] = tmetrics
		// initialize the topic names for holding write/s rolling averages.
		_, ok := s.writesRollingAvg[topic]
		if !ok {
			s.writesRollingAvg[topic] = 0
			s.writesAggWindow[topic] = rolling.NewPointPolicy(rolling.NewWindow(int(s.metadata.RollingMeasurements)))
		} else {
			s.writesAggWindow[topic].Append(tmetrics.WriteRate)
		}
	}
	// important, update the time we gathered partititon metrics for rates calculations
	s.lastOffetsTime = now

	// log the metrics aggregated for all the topics in the Consumer Group

	for name, topicMetrics := range s.topicMetrics {
		above := ""
		if topicMetrics.LagRatio > s.metadata.LagRatio {
			above = " ABOVE THRESHOLD"
		}
		s.logger.V(0).Info(fmt.Sprintf("LagRatio: %.3f%s, Topic %s, Write/s %.3f, Read/s %.3f, Lag: %d, ResidualLag: %d, partitions total/with lag: %d/%d, Group: %s interval(ms): %d",
			topicMetrics.LagRatio, above, name, topicMetrics.WriteRate*1000, topicMetrics.ReadRate*1000, topicMetrics.Lag, topicMetrics.ResidualLag, topicMetrics.PartitionsTotal, topicMetrics.PartitionsWithLag, s.metadata.Group, topicMetrics.Period))
	}
	return nil
}

type ConsumerGroupState int

const (
	DescribeFailed = iota
	NoMembers
	//
	Stable
	Empty
	PreparingRebalance
	CompletingRebalance
	Dead
)

type consumerGroupError struct {
	msg        string // description of error
	groupState ConsumerGroupState
}

func (e *consumerGroupError) Error() string { return e.msg }
func (e *consumerGroupError) PermanentError() bool {
	switch e.groupState {
	case DescribeFailed, NoMembers, Empty, Dead:
		return true
	default:
		return false
	}
}

func (s *kafkaStreamsScaler) getTopicPartitions(ctx context.Context) (map[string][]int, string, int64, int64, error) {
	// Step 1 - get consumer group state and list of topics in the group
	describeGrpReq := &kafka.DescribeGroupsRequest{
		Addr: s.client.Addr,
		GroupIDs: []string{
			s.metadata.Group,
		},
	}
	// call to the broker
	describeGrp, err := s.client.DescribeGroups(ctx, describeGrpReq)
	if err != nil {
		e := consumerGroupError{fmt.Sprintf("error describing group: %s: %s", s.metadata.Group, err), DescribeFailed}
		return nil, "", 0, 0, &e
	}
	if len(describeGrp.Groups[0].Members) == 0 {
		e := consumerGroupError{fmt.Sprintf("no active members in group %s, group-state is %s", s.metadata.Group, describeGrp.Groups[0].GroupState), NoMembers}
		return nil, "", 0, 0, &e
	}
	// Requesting a single group, expecting a single response
	groupState := describeGrp.Groups[0].GroupState
	s.logger.V(2).Info(fmt.Sprintf("Consumer Group %s is in state %s", s.metadata.Group, groupState))
	groupMembersCnt := int64(len(describeGrp.Groups[0].Members))
	s.logger.V(2).Info(fmt.Sprintf("Consumer Group %s has %d members", s.metadata.Group, groupMembersCnt))
	if groupState == "Dead" {
		e := consumerGroupError{fmt.Sprintf("Consumer group %s state is %s", s.metadata.Group, groupState), Dead}
		return nil, "", 0, 0, &e
	}
	if groupState == "Empty" {
		e := consumerGroupError{fmt.Sprintf("Consumer group %s state is %s", s.metadata.Group, groupState), Empty}
		return nil, "", 0, 0, &e
	}

	// This is sufficient in normal conditions:
	//      describeGrp.Groups[0].Members[0].MemberMetadata.Topics
	// but during rebalancing, Members[0] can have no topic.  It may be missing a topic.
	// map for speed to get all the topics from all the members, and make it into an array for kafka-go API
	// also records the number of hosts in the consumer group
	topicsInGroup := make(map[string]struct{})
	hostsInGroup := make(map[string]struct{})
	for _, member := range describeGrp.Groups[0].Members {
		hostsInGroup[member.ClientHost] = struct{}{}
		for _, topic := range member.MemberMetadata.Topics {
			if _, ok := topicsInGroup[topic]; !ok {
				if len(s.metadata.Topics) > 0 {
					// Only topics specified in the config will be used
					for _, config_topic := range s.metadata.Topics {
						if topic == config_topic {
							topicsInGroup[topic] = struct{}{}
						}
					}
				} else {
					match := false
					for _, substr := range s.metadata.IgnoreTopicsMatching {
						if strings.Contains(topic, substr) {
							match = true
							break
						}
					}
					if !match {
						topicsInGroup[topic] = struct{}{}
					}
				}
			}
		}
	}

	// TODO need a better solution - hack
	// host returned by describe consumer group is the worker node ip or name.  hostsCnt value we need is process or pod count, not worker node count.
	// Scaler is not currently configured with the number of streaming theaads; this is more configuration that can get wrong.
	// if no 2 pods of the same service are scheduled on the same worker node, groupMembersCnt / len(hostsInGroup) => number of streaming threads.
	// eg, number of threads = 3, number of members = 120, number of hosts = 40, 120/40 = 3.0.
	// But if lets say 2 pods get scheduled on the same worker node, we get:
	// number of threads = 3, number of members = 120, number of hosts = 39, 120/39 = 3.076923
	// The funny math will restore hostsCnt to 40 pods/process.   Obviously, this will break once we reach 5 hosts with 2 pods each, at
	// which point we have
	// number of members = 120, number of hosts = 30, number of threads = 4.0 and hostsCnt := 30
	// this is only used to avoid the scaler to scale down after minMembersScaleDownFloor. if this is confiured the same value
	// as hpa min replicas, it wont scale lower anyways.   this is just a cosmetric issue.
	hostsCnt := int64(float64(groupMembersCnt) / math.Floor(float64(groupMembersCnt)/float64(len(hostsInGroup))))

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
		if len(s.metadata.Topics) > 0 && !kedautil.Contains(s.metadata.Topics, topic.Name) {
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
	partitionMetrics.residualLag = int64(partitionMetrics.writeRate * float64(s.metadata.CommitInterval) / 2.0)
	s.logger.V(1).Info(fmt.Sprintf("ResidualLag %d for topic partion %s:%d", partitionMetrics.residualLag, topic, partitionID))
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
		TLS:         tlsConfig,
		SASL:        saslMechanism,
		IdleTimeout: time.Second * 300,
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

// We need just one producer for all the kafka-streams scaler instance
// there is no place for a common initialization for all scalers of one kind in Keda,
// hence this Mutex.

// TODO: no SASL code.
func (w *kedaKafkaProducer) createProducer(bootstrapServers []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.producer != nil {
		// antoher scale object initializer the share writer.
		return
	}
	producer := kafka.Writer{
		Addr:         kafka.TCP(bootstrapServers...),
		Topic:        kedaKafaStreamsTopic,
		BatchSize:    1,
		RequiredAcks: kafka.RequireAll,
	}
	w.producer = &producer
}

// creates if necessary the compacted topic to store metrics. There is one small json per scale object needed,
// only the most recent one. 1 partition is plenty.  Json blob upded only at scale up time
// Called from New... returning an error will fail the SO, will go to Active=false
func createProducerTopic(client *kafka.Client) (err error) {
	config := []kafka.ConfigEntry{{
		ConfigName:  "cleanup.policy",
		ConfigValue: "compact",
	}}
	res, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             kedaKafaStreamsTopic,
				NumPartitions:     1,
				ReplicationFactor: 3,
				ConfigEntries:     config,
			},
		},
	})
	// the  call can fail of it can fail for just one of the topics, although we are using only 1
	if err != nil {
		return err
	}
	for _, error := range res.Errors {
		if error != nil {
			return err
		}
	}
	return nil
}

// The scaler now just use the metrics that cross threshold on upscale to down scale
// save just that metric to kafka; trivial to add the other topics if they come necessary later.
func (w *kedaKafkaProducer) publishConsumerGroupMetrics(s *kafkaStreamsScaler, topicName string, tMetrics *kafkaTopicMetrics) {
	groupName := s.metadata.Group

	p := persistentTopicMetric{
		TopicName:   topicName,
		TopicMetric: *tMetrics,
	}
	msg, err := json.Marshal(p)
	if err != nil {
		s.logger.V(0).Info("Compacted topic write error: unexpected marshalling error")
	}

	// ctx, cancel := context.WithTimeout(context.Background(), Duration(time.Millisecond*1500)time.)
	// defer cancel()
	ctx := context.Background()
	err = w.producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(groupName),
		Value: []byte(msg),
	})
	if err != nil {
		s.logger.V(0).Info(fmt.Sprintf("Compacted topic write error: %s", err.Error()))
	}
}

// Done at startup only.   Whenever those metrics are updated, they are stored in in-memory scale state and published
// to the compacted topic.  Also, we need to read the compacted topic ONCE at Keda startup, there is no hook for that
// so every scale object will call.
func (p *kafkaStreamsPersistentMetrics) ReadCompactedTopic(bootstrapServers []string, logger logr.Logger) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() { p.initialized = true }()
	if p.initialized {
		return nil
	}

	logger.V(1).Info(fmt.Sprintf("Compacted Topic - Creating Reader for tpoic %s", kedaKafaStreamsTopic))
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     bootstrapServers,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		Topic:       kedaKafaStreamsTopic,
		MaxBytes:    10e6, // 10MB
		MaxWait:     time.Millisecond * 100,
	})

	// Go being go...
	foo := make(map[string]persistentTopicMetric)
	p.savedMetrics = foo

	// context for FetchMessage to return when there is nothing more to read on the compated topic.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*100))
	defer cancel()
	for {
		topicMetric := persistentTopicMetric{}
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			logger.V(1).Info(fmt.Sprintf("Compacted Topic - FetchMessage() completed: %s", err.Error()))
			break
		}
		logger.V(1).Info(fmt.Sprintf("Compacted Topic - message message read at topic/partition/offset/high WAtermark %v/%v/%v/%v: %s = %s\n",
			m.Topic, m.Partition, m.Offset, m.HighWaterMark, string(m.Key), string(m.Value)))

		if err := json.Unmarshal([]byte(m.Value), &topicMetric); err != nil {
			fmt.Printf("Compacted Topic - message error unmarshaling JSON: %v\n", err)
		}
		// highest offset value will have the most recent key
		p.savedMetrics[string(m.Key)] = topicMetric
	}
	if err := reader.Close(); err != nil {
		return err
	}
	return nil
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
