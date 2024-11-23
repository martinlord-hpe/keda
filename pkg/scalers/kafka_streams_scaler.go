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

// Please note that this is an experimental scaler based on the kafka-go library.

package scalers

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	//	"math"
	"strings"
	//	"time"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	awsutils "github.com/kedacore/keda/v2/pkg/scalers/aws"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type kafkaStreamsScaler struct {
	metricType v2.MetricTargetType
	metadata   *kafkaStreamsMetadata
	client     *kafka.Client
	logger     logr.Logger
	// Scaler state
	previousConsumerOffsets map[string]map[int]int64
	previousLastOffsets     map[string]map[int]int64
	// TODO record this state by topic
	/*
		lastOffetsTime        int64
		thresholdCountUp      int64
		thresholdCountDown    int64
		wThrougoutLastScaleUp float64
		topicNameLastScaleUp  string
		pollingCount          int64
		pollingStableCount    int64
	*/
}

const (
	// invalidPartitionOffset = int64(-1)
	// default Values for trigger parameters
	defaultLagRatio                  = 3.0
	defaultCommitInterval            = 30000
	defaultMinPartitionWriteThrouput = 0.5
	defaultMeasurementsForScale      = 3
	defaultScaleDownFactor           = 0.75
	defaultAllowIdleConsumers        = false
	defaultLimitToPartitionsWithLag  = true
)

type kafkaStreamsMetadata struct {
	// Madatory Metadata
	BootstrapServers []string
	Group            string
	// Optional Metadata with no default values
	Topic []string
	// Optional Metadata with default values
	LagRatio                  float64
	CommitInterval            int64
	MinPartitionWriteThrouput float64
	MeasurementsForScale      int64
	ScaleDownFactor           float64
	AllowIdleConsumers        bool
	LimitToPartitionsWithLag  bool

	// Authenticaltion, copied from apache-kafka implementation
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
	// TODO
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
		if err != nil || lagRatio <= 0.0 {
			return nil, fmt.Errorf("lagRatio must be a float greater than 1.0")
		}
		meta.LagRatio = lagRatio
	} else {
		meta.LagRatio = defaultLagRatio
	}

	if val, ok := config.TriggerMetadata["commitInterval"]; ok {
		commitInterval, err := strconv.ParseInt(val, 10, 64)
		if err != nil || float64(commitInterval) <= 0 {
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

	if val, ok := config.TriggerMetadata["allowIdleConsumers"]; ok {
		idle, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("allowIdleConsumers must be \"true\" or \"false\"")
		}
		meta.AllowIdleConsumers = idle
	} else {
		meta.AllowIdleConsumers = defaultAllowIdleConsumers
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

	// TODO: parse Authentication (TLS, SASL,MSK)
	meta.SASLType = KafkaSASLTypeNone

	// meta. meta.ScalerIndexIndex = config.ScalerIndex
	return &meta, nil
}

// NewkafkaStreamScaler creates a new kafkaStreamScaler
func NewKafkaStreamScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	meta, err := parseKafkaStreamsMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing kafka streams scaler metadata: %w", err)
	}

	logger := InitializeLogger(config, "kafka_streams_scaler")
	client, err := getKafkaGoClient(ctx, *meta, logger)
	if err != nil {
		return nil, err
	}

	previousConsumerOffsets := make(map[string]map[int]int64)
	previousLastOffsets := make(map[string]map[int]int64)

	return &kafkaStreamsScaler{
		client:                  client,
		metricType:              metricType,
		metadata:                meta,
		logger:                  logger,
		previousConsumerOffsets: previousConsumerOffsets,
		previousLastOffsets:     previousLastOffsets,
		// topicNameLastScaleUp:    "Not Set",
	}, nil
}

// Scaler Interface GetMetricsAndActivity()
func (s *kafkaStreamsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	// TO DO
	s.logger.V(0).Info("GetMetricsAndActivity!!!")

	metric := GenerateMetricInMili(metricName, 0.0)
	return []external_metrics.ExternalMetricValue{metric}, true, nil

	/*

		toplLagRatio, _, err := s.getTotalLagRatio(ctx)
		if err != nil {
			s.logger.V(5).Info(fmt.Sprintf("GetMetricsAndActivity err %s", err))
			return []external_metrics.ExternalMetricValue{}, false, err
		} else {
			s.logger.V(0).Info(fmt.Sprintf("GetMetricsAndActivity topLagRatio %.3f", toplLagRatio))
		}

		metric := GenerateMetricInMili(metricName, toplLagRatio)
		return []external_metrics.ExternalMetricValue{metric}, true, nil
	*/

}

// Scaler Interface GetMetricSpecForScaling()
func (s *kafkaStreamsScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	s.logger.V(0).Info("GetMetricSpecForScaling!!!")

	metricName := fmt.Sprintf("kafka-streams-%s-topics", s.metadata.Group)
	// TODO define a better HPA TARGER
	metricTarget := s.metadata.LagRatio
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(metricName)),
		},
		Target: GetMetricTargetMili(s.metricType, metricTarget),
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: "External"}
	s.logger.V(0).Info(fmt.Sprintf("kafka-streams metric name: %s", metricName))
	return []v2.MetricSpec{metricSpec}
}

// Close closes the kafka client
func (s *kafkaStreamsScaler) Close(context.Context) error {
	s.logger.V(0).Info("Close!!!")

	if s.client == nil {
		return nil
	}
	transport := s.client.Transport.(*kafka.Transport)
	if transport != nil {
		transport.CloseIdleConnections()
	}
	return nil
}

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

/*

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



func (s *apacheKafkaScaler) getTopicPartitions(ctx context.Context) (map[string][]int, string, error) {
	metadata, err := s.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: s.client.Addr,
	})
	if err != nil {
		return nil, "", fmt.Errorf("error getting metadata: %w", err)
	}
	s.logger.V(3).Info(fmt.Sprintf("Listed topics %v", metadata.Topics))

	var groupState string
	if len(s.metadata.Topic) == 0 {
		// in case of empty topic name, we will get all topics that the consumer group is subscribed to
		describeGrpReq := &kafka.DescribeGroupsRequest{
			Addr: s.client.Addr,
			GroupIDs: []string{
				s.metadata.Group,
			},
			// TODO: added to make response compabible but does not seem to be helping with the version 3 responce,
			IncludeAuthorizedOperations: 0,
		}

		describeGrp, err := s.client.DescribeGroups(ctx, describeGrpReq)

		if err != nil {
			return nil, groupState, fmt.Errorf("error describing group: %w", err)
		}
		if len(describeGrp.Groups[0].Members) == 0 {
			return nil, groupState, fmt.Errorf("no active members in group %s, group-state is %s", s.metadata.Group, describeGrp.Groups[0].GroupState)
		}
		s.logger.V(4).Info(fmt.Sprintf("Described group %s with response %v", s.metadata.Group, describeGrp))

		result := make(map[string][]int)

		topicsInGroup := describeGrp.Groups[0].Members[0].MemberMetadata.Topics
		groupState = describeGrp.Groups[0].GroupState
		s.logger.V(1).Info(fmt.Sprintf("Consumer Group %s is in state %s", s.metadata.Group, groupState))

		for _, topic := range metadata.Topics {
			partitions := make([]int, 0)
			if kedautil.Contains(topicsInGroup, topic.Name) {
				s.logger.V(4).Info(fmt.Sprintf("topic name found in froup: %s", topic.Name))

				for _, partition := range topic.Partitions {
					// if no partitions limitatitions are specified, all partitions are considered
					if (len(s.metadata.PartitionLimitation) == 0) ||
						(len(s.metadata.PartitionLimitation) > 0 && kedautil.Contains(s.metadata.PartitionLimitation, partition.ID)) {
						partitions = append(partitions, partition.ID)
					}
				}
			}
			result[topic.Name] = partitions
		}
		return result, groupState, nil
	}

	// TODO refactor this, support group state with topic option, not used today, but should be fixed
	result := make(map[string][]int)
	for _, topic := range metadata.Topics {
		partitions := make([]int, 0)
		if kedautil.Contains(s.metadata.Topic, topic.Name) {
			for _, partition := range topic.Partitions {
				if (len(s.metadata.PartitionLimitation) == 0) ||
					(len(s.metadata.PartitionLimitation) > 0 && kedautil.Contains(s.metadata.PartitionLimitation, partition.ID)) {
					partitions = append(partitions, partition.ID)
				}
			}
		}
		result[topic.Name] = partitions
	}
	return result, groupState, nil
}

func (s *apacheKafkaScaler) getConsumerOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, error) {
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
func (s *apacheKafkaScaler) getProducerOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, error) {
	// Step 1: build one OffsetRequest
	offsetRequest := make(map[string][]kafka.OffsetRequest)

	for topic, partitions := range topicPartitions {
		for _, partitionID := range partitions {
			offsetRequest[topic] = append(offsetRequest[topic], kafka.FirstOffsetOf(partitionID), kafka.LastOffsetOf(partitionID))
		}
	}

	// Step 2: send request
	res, err := s.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   s.client.Addr,
		Topics: offsetRequest,
	})
	if err != nil {
		return nil, err
	}

	// Step 3: parse response and return
	producerOffsets := make(map[string]map[int]int64)
	for topic, partitionOffset := range res.Topics {
		producerOffsets[topic] = make(map[int]int64)
		for _, partition := range partitionOffset {
			producerOffsets[topic][partition.Partition] = partition.LastOffset
		}
	}

	return producerOffsets, nil
}


func (s *apacheKafkaScaler) getLagForPartition(topic string, partitionID int, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (int64, int64, error) {
	if len(consumerOffsets) == 0 {
		return 0, 0, fmt.Errorf("consumerOffsets is empty")
	}
	if len(producerOffsets) == 0 {
		return 0, 0, fmt.Errorf("producerOffsets is empty")
	}

	consumerOffset := consumerOffsets[topic][partitionID]
	if consumerOffset == invalidPartitionOffset && s.metadata.OffsetResetPolicy == latest {
		retVal := int64(1)
		if s.metadata.ScaleToZeroOnInvalidOffset {
			retVal = 0
		}
		msg := fmt.Sprintf(
			"invalid offset found for topic %s in group %s and partition %d, probably no offset is committed yet. Returning with lag of %d",
			topic, s.metadata.Group, partitionID, retVal)
		s.logger.V(1).Info(msg)
		return retVal, retVal, nil
	}

	if _, found := producerOffsets[topic]; !found {
		return 0, 0, fmt.Errorf("error finding partition offset for topic %s", topic)
	}
	producerOffset := producerOffsets[topic][partitionID]

	if consumerOffset == invalidPartitionOffset && s.metadata.OffsetResetPolicy == earliest {
		if s.metadata.ScaleToZeroOnInvalidOffset {
			return 0, 0, nil
		}
		return producerOffset, producerOffset, nil
	}

	// This code block tries to prevent KEDA Kafka trigger from scaling the scale target based on erroneous events
	if s.metadata.ExcludePersistentLag {
		switch previousOffset, found := s.previousConsumerOffsets[topic][partitionID]; {
		case !found:
			// No record of previous offset, so store current consumer offset
			// Allow this consumer lag to be considered in scaling
			if _, topicFound := s.previousConsumerOffsets[topic]; !topicFound {
				s.previousConsumerOffsets[topic] = map[int]int64{partitionID: consumerOffset}
			} else {
				s.previousConsumerOffsets[topic][partitionID] = consumerOffset
			}
		case previousOffset == consumerOffset:
			// Indicates consumer is still on the same offset as the previous polling cycle, there may be some issue with consuming this offset.
			// return 0, so this consumer lag is not considered for scaling
			return 0, producerOffset - consumerOffset, nil
		default:
			// Successfully Consumed some messages, proceed to change the previous offset
			s.previousConsumerOffsets[topic][partitionID] = consumerOffset
		}
	}

	s.logger.V(4).Info(fmt.Sprintf("Consumer offset for topic %s in group %s and partition %d is %d", topic, s.metadata.Group, partitionID, consumerOffset))
	s.logger.V(4).Info(fmt.Sprintf("Producer offset for topic %s in group %s and partition %d is %d", topic, s.metadata.Group, partitionID, producerOffset))

	return producerOffset - consumerOffset, producerOffset - consumerOffset, nil
}

func (s *apacheKafkaScaler) getCurrentAndUpdatePreivouOffsets(topic string, partitionID int, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (int64, int64, int64, int64) {

	consumerOffset := invalidPartitionOffset
	previousConsumerOffset := invalidPartitionOffset
	producerOffset := invalidPartitionOffset
	previousProducerOffset := invalidPartitionOffset
	var found bool

	if len(consumerOffsets) != 0 {
		consumerOffset = consumerOffsets[topic][partitionID]
		previousConsumerOffset, found = s.previousConsumerOffsets[topic][partitionID]
		s.logger.V(0).Info(fmt.Sprintf("Got Committed Offset %d, Previous Committed Offset %d for %s:%d, found: %v", consumerOffset, previousConsumerOffset, topic, partitionID, found))
		switch {
		case !found:
			// No record of previous comitted offset, just store current topic and committed offset
			if _, topicFound := s.previousConsumerOffsets[topic]; !topicFound {
				s.previousConsumerOffsets[topic] = map[int]int64{partitionID: consumerOffset}
			} else {
				s.previousConsumerOffsets[topic][partitionID] = consumerOffset
			}
			previousConsumerOffset = invalidPartitionOffset
		default:
			s.previousConsumerOffsets[topic][partitionID] = consumerOffset
		}
	}

	if len(producerOffsets) != 0 {
		producerOffset = producerOffsets[topic][partitionID]
		previousProducerOffset, found = s.previousLastOffsets[topic][partitionID]
		s.logger.V(0).Info(fmt.Sprintf("Got last Offset %d, Previous last Offset %d for %s:%d, found: %v", producerOffset, previousProducerOffset, topic, partitionID, found))
		switch {
		case !found:
			// No record of previous last offset, store current producer offset
			if _, topicFound := s.previousLastOffsets[topic]; !topicFound {
				s.previousLastOffsets[topic] = map[int]int64{partitionID: producerOffset}
			} else {
				s.previousLastOffsets[topic][partitionID] = producerOffset
			}
			previousProducerOffset = invalidPartitionOffset
		default:
			s.previousLastOffsets[topic][partitionID] = producerOffset
		}
	}

	s.logger.V(1).Info(fmt.Sprintf("Offsets for group %s topic partition %s:%d, , Last Offset %d, Previous Last Offset %d Committed Offset %d, Previous Committed Offset %d", s.metadata.Group, topic, partitionID, producerOffset, previousProducerOffset, consumerOffset, previousConsumerOffset))

	return consumerOffset, previousConsumerOffset, producerOffset, previousProducerOffset

}


// # TODO excludePersistentLag - should implement, trivial now that we calculate read/s. It does not make sense to scale on zero

func (s *apacheKafkaScaler) getLagRatioForPartition(topic string, partitionID int, now int64, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (float64, float64, float64, int64, int64, error) {
	// Read all the offsets for the topic partition, update previous offsets.
	consumerOffset, previousConsumerOffset, producerOffset, previousProducerOffset := s.getCurrentAndUpdatePreivouOffsets(topic, partitionID, consumerOffsets, producerOffsets)
	if previousConsumerOffset == invalidPartitionOffset || previousProducerOffset == invalidPartitionOffset {
		s.logger.V(1).Info(fmt.Sprintf("Previous offsets not available (perhaps first check?), cannot compute metrics"))
		// TODO
		return 0, 0, 0, 0, 0, nil
	}
	if consumerOffset == invalidPartitionOffset || producerOffset == invalidPartitionOffset {
		s.logger.V(1).Info(fmt.Sprintf("Current offsets could not be read, cannot compute metrics"))
		// TODO
		return 0, 0, 0, 0, 0, nil
	}
	previousLastOffsettime := s.lastOffetsTime
	period := now - previousLastOffsettime
	if period <= 0 {
		// TODO
		return 0, 0, 0, 0, 0, fmt.Errorf("unexpected error calculating period for topic partition %s:%d", topic, partitionID)
	}

	// Write throughput on the partition in messages per milliseconds
	writtenMsg := producerOffset - previousProducerOffset
	if writtenMsg < 0 {
		// TODO
		return 0, 0, 0, 0, 0, fmt.Errorf("unexpected error calculating messages/s for topic partition %s:%d", topic, partitionID)
	}
	writeThroughput := float64(writtenMsg) / float64(period)
	// Read throughput on the partition in messages per milliseconds
	readMsg := consumerOffset - previousConsumerOffset
	if readMsg < 0 {
		// TODO
		return 0, 0, 0, 0, 0, fmt.Errorf("unexpected error calculating messages/s for topic partition %s:%d", topic, partitionID)
	}
	readThroughput := float64(readMsg) / float64(period)
	s.logger.V(1).Info(fmt.Sprintf("%.3f writes/s, %.3f reads/s for last %.3f seconds for topic partion %s:%d", writeThroughput*1000, readThroughput*1000, float64(period)/1000, topic, partitionID))

	// residualLag is the lag we are expecting to see even if the consumerGroup reads the messages on a
	// timely basis.  The higher the partition throughput is and the highger the commit interval is
	// (30,000ms defautt for Kafka Streams), the higher the residualLag.    This is why scaling on
	// topic partitions fixed lag value is not possible for data pipelines, the absolute lag value
	// will increase with write throughout. The premise of this scaler is that lagRatio will stay constant
	// at an average of 0.5 as long as the consumerGroup will be able to consume the messages and lagRatio
	// will start to go up when consumerGroup will fall behind.
	//
	// On well balanced topics with some throughput in the tens of messages per second, lag will measure
	// slightly above 0.5. Depending on the luck of the draw, it could come anywhere between 0.0 and slightly
	// above 1.0 depending when we read the offsets in relation to when consumer offsets are updated.   producer
	// offsets are updated as the messages are written
	partitionLag := producerOffset - consumerOffset
	ratio := 0.0
	residualLag := 0.0
	if writeThroughput*1000 > s.metadata.MinPartitionWriteThrouput {
		residualLag = writeThroughput * float64(s.metadata.CommitInterval) / 2.0
		ratio = float64(producerOffset-consumerOffset) / residualLag
	} else {
		// returning write throughput for future scale down decisions, make it zero if under MinPartitionWriteThrouput
		writeThroughput = 0
	}

	if s.metadata.ExcludePersistentLag {
		// TODO: implement this
	}

	s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio %.6f based on residualLag %.6f for topic partion %s:%d", ratio, residualLag, topic, partitionID))

	return ratio, writeThroughput, readThroughput, partitionLag, int64(residualLag), nil
}

// Close closes the kafka client
func (s *apacheKafkaScaler) Close(context.Context) error {
	if s.client == nil {
		return nil
	}
	transport := s.client.Transport.(*kafka.Transport)
	if transport != nil {
		transport.CloseIdleConnections()
	}
	return nil
}

func (s *apacheKafkaScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	var metricName string

	if s.metadata.LagRatio != 0.0 {
		if s.metadata.Topic != nil && len(s.metadata.Topic) > 0 {
			metricName = fmt.Sprintf("kafka-lagratio-%s-%s", s.metadata.Group, strings.Join(s.metadata.Topic, ","))
		} else {
			metricName = fmt.Sprintf("kafka-lagratio-%s-topics", s.metadata.Group)
		}

		externalMetric := &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(metricName)),
			},
			Target: GetMetricTargetMili(s.metricType, s.metadata.LagRatio),
		}
		metricSpec := v2.MetricSpec{External: externalMetric, Type: "External"}
		s.logger.V(2).Info(fmt.Sprintf("Kafka lag Ratio metric name: %s", metricName))
		return []v2.MetricSpec{metricSpec}

	} else {
		if s.metadata.Topic != nil && len(s.metadata.Topic) > 0 {
			metricName = fmt.Sprintf("kafka-%s", strings.Join(s.metadata.Topic, ","))
		} else {
			metricName = fmt.Sprintf("kafka-%s-topics", s.metadata.Group)
		}

		externalMetric := &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{
				Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, kedautil.NormalizeString(metricName)),
			},
			Target: GetMetricTarget(s.metricType, s.metadata.LagThreshold),
		}
		metricSpec := v2.MetricSpec{External: externalMetric, Type: "External"}
		s.logger.V(2).Info(fmt.Sprintf("Kafka lag Threshold metric name: %s", metricName))
		return []v2.MetricSpec{metricSpec}
	}
}

type apacheKafkaConsumerOffsetResult struct {
	consumerOffsets map[string]map[int]int64
	err             error
}

type apacheKafkaProducerOffsetResult struct {
	producerOffsets map[string]map[int]int64
	err             error
}

// getConsumerAndProducerOffsets returns (consumerOffsets, producerOffsets, error)
func (s *apacheKafkaScaler) getConsumerAndProducerOffsets(ctx context.Context, topicPartitions map[string][]int) (map[string]map[int]int64, map[string]map[int]int64, error) {
	consumerChan := make(chan apacheKafkaConsumerOffsetResult, 1)
	go func() {
		consumerOffsets, err := s.getConsumerOffsets(ctx, topicPartitions)
		consumerChan <- apacheKafkaConsumerOffsetResult{consumerOffsets, err}
	}()

	producerChan := make(chan apacheKafkaProducerOffsetResult, 1)
	go func() {
		producerOffsets, err := s.getProducerOffsets(ctx, topicPartitions)
		producerChan <- apacheKafkaProducerOffsetResult{producerOffsets, err}
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



// getTotalLag returns totalLag, totalLagWithPersistent, error
// totalLag and totalLagWithPersistent are the summations of lag and lagWithPersistent returned by getLagForPartition function respectively.
// totalLag maybe less than totalLagWithPersistent when excludePersistentLag is set to `true` due to some partitions deemed as having persistent lag
func (s *apacheKafkaScaler) getTotalLag(ctx context.Context) (int64, int64, error) {
	s.pollingCount++
	topicPartitions, _, err := s.getTopicPartitions(ctx)
	if err != nil {
		return 0, 0, err
	}
	s.logger.V(1).Info(fmt.Sprintf("Kafka scaler: Topic partitions %v", topicPartitions))

	consumerOffsets, producerOffsets, err := s.getConsumerAndProducerOffsets(ctx, topicPartitions)
	s.logger.V(4).Info(fmt.Sprintf("Kafka scaler: Consumer offsets %v, producer offsets %v", consumerOffsets, producerOffsets))
	if err != nil {
		return 0, 0, err
	}

	totalLag := int64(0)
	totalLagWithPersistent := int64(0)
	totalTopicPartitions := int64(0)
	partitionsWithLag := int64(0)

	for topic, partitionsOffsets := range producerOffsets {
		for partition := range partitionsOffsets {
			lag, lagWithPersistent, err := s.getLagForPartition(topic, partition, consumerOffsets, producerOffsets)
			if err != nil {
				return 0, 0, err
			}
			totalLag += lag
			totalLagWithPersistent += lagWithPersistent

			if lag > 0 {
				partitionsWithLag++
			}
		}
		totalTopicPartitions += (int64)(len(partitionsOffsets))
	}
	s.logger.V(1).Info(fmt.Sprintf("Kafka scaler: Providing metrics based on totalLag %v, topicPartitions %v, threshold %v", totalLag, topicPartitions, s.metadata.LagThreshold))
	s.logger.V(1).Info(fmt.Sprintf("Kafka scaler: Consumer offsets %v, producer offsets %v", consumerOffsets, producerOffsets))

	if !s.metadata.AllowIdleConsumers || s.metadata.LimitToPartitionsWithLag {
		// don't scale out beyond the number of topicPartitions or partitionsWithLag depending on settings
		upperBound := totalTopicPartitions
		if s.metadata.LimitToPartitionsWithLag {
			upperBound = partitionsWithLag
		}

		if (totalLag / s.metadata.LagThreshold) > upperBound {
			totalLag = upperBound * s.metadata.LagThreshold
		}
	}
	return totalLag, totalLagWithPersistent, nil
}

func (s *apacheKafkaScaler) getTotalLagRatio(ctx context.Context) (float64, float64, error) {
	s.pollingCount++
	topicPartitions, groupState, err := s.getTopicPartitions(ctx)
	if err != nil {
		return 0, 0, err
	}

	consumerOffsets, producerOffsets, err := s.getConsumerAndProducerOffsets(ctx, topicPartitions)
	s.logger.V(5).Info(fmt.Sprintf("Kafka scaler: Consumer offsets %v, producer offsets %v", consumerOffsets, producerOffsets))
	if err != nil {
		return 0, 0, err
	}

	// aggregate partitions metrics into topic metrics
	var topicLag, topicResidualLag, partitionsWithLag, topicPartitionNum int64
	var topicLagRatio, topicWriteThroughput, topicReadThroughput float64
	// aggregate topic metrics into consumer group metrics
	var topicLargestLag, topicLagestResidualLag int64
	var topicLargestRatio, topicLargestWriteThroughput, topicLargestReadThroughput float64
	topicNameLargestRatio := ""

	// used to record approximate period since last metrics check to calculate per partition write throughout
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for topic, partitionsOffsets := range producerOffsets {
		topicLag = 0
		topicResidualLag = 0
		topicLagRatio = 0
		for partition := range partitionsOffsets {
			lagRatio, writeThroughput, readThroughput, partitionLag, partitionResidualLag, err := s.getLagRatioForPartition(topic, partition, now, consumerOffsets, producerOffsets)
			if err != nil {
				return 0.0, 0.0, err
			}
			topicLag += partitionLag
			topicResidualLag += partitionResidualLag
			topicWriteThroughput += writeThroughput
			topicReadThroughput += readThroughput
			// lagRaio sum is meaningless, divided by number of paritions later
			if lagRatio > 0 {
				topicLagRatio += lagRatio
				partitionsWithLag++
			}
		}

		// Averable lag ratio overl all partitions or only those with a lag, if imitToPartitionsWithLag is configured.
		topicPartitionNum = (int64)(len(partitionsOffsets))
		if s.metadata.LimitToPartitionsWithLag {
			if partitionsWithLag == 0 {
				topicLagRatio = 0
			} else {
				topicLagRatio /= float64(partitionsWithLag)
			}
			s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio, lagRatio with only %d partitions with lag: %.6f for group: %s, topic: %s", partitionsWithLag, topicLagRatio, s.metadata.Group, topic))
		} else {
			topicLagRatio /= float64(topicPartitionNum)
			s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio, lagRatio average across all partitions: %.6f for group: %s, topic: %s", topicLagRatio, s.metadata.Group, topic))
		}

		// At consumer group level, we collect the metrics values for the topic with the largest lagRatio
		if topicLagRatio > topicLargestRatio {
			topicNameLargestRatio = topic
			topicLargestRatio = topicLagRatio
			topicLargestWriteThroughput = topicWriteThroughput
			topicLargestReadThroughput = topicReadThroughput
			topicLargestLag = topicLag
			topicLagestResidualLag = topicResidualLag
		}

		// scale down metrics.
		if topic == s.topicNameLastScaleUp {
			// Scaling down when write throughput goes down 'enough' from where it was in the scale up.
			if topicReadThroughput > 0 && topicWriteThroughput > 0 && topicWriteThroughput < s.wThrougoutLastScaleUp*s.metadata.ScaleDownFactor {
				margin := 0.8 // arbitrary
				// Scale down only if read throughput does not fall under write throuhput.
				if topicReadThroughput > topicWriteThroughput*margin {
					s.thresholdCountDown++
				}
			} else {
				s.thresholdCountDown = 0
			}
		}
	}

	s.lastOffetsTime = now

	if topicNameLargestRatio != "" {
		s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, largest ratio %.6f for group: %s is in topic %s, threshold %.6f", topicLagRatio, s.metadata.Group, topicNameLargestRatio, s.metadata.LagRatio))
	} else {
		s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, group: %s has no topic with lagRatio > 0.0", s.metadata.Group))
	}

	hpaMetric := float64(1.0)
	cappedLogRatio := topicLargestRatio
	if cappedLogRatio > s.metadata.LagRatio {
		// above scale up threshold, reset scale down.
		s.thresholdCountDown = 0

		if s.thresholdCountUp++; s.thresholdCountUp >= s.metadata.MeasurementsForScale {
			if topicLargestReadThroughput > topicLargestWriteThroughput {
				// LagRatio still indicates we are behind, but if read throughput is greate than write
				// we are catching up.    if the time to catch up is inferior to lagRecoveryTime
				// we will not add replicas,
				realLag := topicLargestLag - topicLagestResidualLag
				// in seconds
				lagTimeToNomimal := float64(realLag) / ((topicLargestReadThroughput * 1000) - (topicLargestWriteThroughput * 1000))
				// 600 seconds, temporary hard code.
				if int64(lagTimeToNomimal) > 600 {
					// reads faster than writes, but not enough,  multiply metric by 1.11
					// let hpa policy add replicas as per the policy (1 ot 10% which ever is bigger kind of thing)
					hpaMetric = 1.11
					s.logger.V(0).Info(fmt.Sprintf("Limiting scale up with HPA Metric multiplier:  %.2f, current lag %d would be nominal in %.0fs at current rate", hpaMetric, realLag, lagTimeToNomimal))
				} else {
					hpaMetric = 1.0
					cappedLogRatio = s.metadata.LagRatio
					s.logger.V(0).Info(fmt.Sprintf("Not scaling up HPA Metric multiplier: 1.0, Current real lag %d would be nominal in %.0fs at current rate", realLag, lagTimeToNomimal))
				}
			} else {
				// LagRatio still indicates we are behind and write throughput greater than read,
				// Calculate some conservative factor

				// HPA metric: desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
				//  avoid too small read throughput, which product large scale up.
				// TODO - add knob for 10 and 0.8 ?
				if topicLargestReadThroughput*1000 > 10 {
					hpaMetric = math.Max(1.11, 0.8*topicLargestWriteThroughput/topicLargestReadThroughput)
				} else {
					hpaMetric = 1.11
				}
				s.logger.V(0).Info(fmt.Sprintf("Calculated HPA Metric multiplier:  %.3f, based on %.1f write/s and %.1f read/s", hpaMetric, topicLargestWriteThroughput*1000, topicLargestReadThroughput*1000))
			}

			if hpaMetric != 1.0 {
				// We are scaling up! reset scale down/up counters.
				s.thresholdCountDown = 0
				s.thresholdCountUp = 0

				cappedLogRatio = hpaMetric * s.metadata.LagRatio

				// scale up event, record topic write through put.
				s.topicNameLastScaleUp = topicNameLargestRatio
				s.wThrougoutLastScaleUp = topicLargestWriteThroughput
				s.logger.V(0).Info(fmt.Sprintf("Kafka lagRatio, Recording read throuput of %.3f on scale up for group: %s is in topic %s", s.wThrougoutLastScaleUp, s.metadata.Group, topicNameLargestRatio))
			}
		} else {
			// we crossed threshold, but not number of measurements requried, just report metric as target
			// this will not cause any scaling.
			cappedLogRatio = s.metadata.LagRatio
		}
	} else {
		// reset scale up, lagRatio under threshold
		s.thresholdCountUp = 0
		if s.thresholdCountDown >= s.metadata.MeasurementsForScale {
			s.thresholdCountDown = 0
			// HPA metric: desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
			// with the above algo in HPA, if we want to down scale from up to 3 to 2, this value must be low
			// using policies to soften
			cappedLogRatio = s.metadata.LagRatio * 0.5
			s.logger.V(0).Info(fmt.Sprintf("Kafka lagRatio, scaling down group: %s on topic %s", s.metadata.Group, topicNameLargestRatio))
		} else {
			cappedLogRatio = s.metadata.LagRatio

		}
	}

	if groupState != "" && groupState != "Stable" {
		s.logger.V(1).Info(fmt.Sprintf("Reset measurements counts for group: %s in state %s", s.metadata.Group, groupState))
		s.thresholdCountUp = 0
		s.thresholdCountDown = 0
	}

	// Scale Down shenanigans.
	if cappedLogRatio == s.metadata.LagRatio {
		// metric = TARGET
		// bad code alert.   this assumes the polling interval = 60s so 30 is for 30 minutes
		// stable no scaling for 30 minutes
		if s.pollingStableCount++; s.pollingStableCount > 30 {
			if s.topicNameLastScaleUp == topicNameLargestRatio { // that's a limitation....
				if topicLargestWriteThroughput > s.wThrougoutLastScaleUp {
					// set curent as new base line.
					s.topicNameLastScaleUp = topicNameLargestRatio
					s.wThrougoutLastScaleUp = topicLargestWriteThroughput
				}
			}
		}
	}
	// one time only
	if s.pollingCount > 5 && s.topicNameLastScaleUp == "Not Set" {
		// record current topic write throughput on startup as a baseline.
		// this is not great, this assumes that the current replicas cont is appropriate for writeThrouhout at scaler start up
		// 5 is arbitrary time to allow for stable metrics.
		s.topicNameLastScaleUp = topicNameLargestRatio
		s.wThrougoutLastScaleUp = topicLargestWriteThroughput
	}

	s.logger.V(0).Info(fmt.Sprintf("HPA Metric: %.3f, Group state:%s, lag ratio: %.3fs, counts up/down: %d/%d, lag:%d, write/s: %.1f, read/s: %.1f, Scale down on topic/throughput: %s/%.1f, group: %s on topic: %s",
		cappedLogRatio, groupState, topicLargestRatio, s.thresholdCountUp, s.thresholdCountDown, topicLargestLag, topicLargestWriteThroughput*1000, topicLargestReadThroughput*1000, s.topicNameLastScaleUp, s.wThrougoutLastScaleUp*1000, s.metadata.Group, topicNameLargestRatio))

	return cappedLogRatio, cappedLogRatio, nil

*/
