/*
Copyright 2023 The KEDA Authors

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
	"strings"
	"time"

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

type apacheKafkaScaler struct {
	metricType            v2.MetricTargetType
	metadata              apacheKafkaMetadata
	client                *kafka.Client
	logger                logr.Logger
	previousOffsets       map[string]map[int]int64
	previousLastOffsets   map[string]map[int]int64
	lastOffetsTime        int64
	thresholdCountUp      int64
	thresholdCountDown    int64
	wThrougoutLastScaleUp float64
	topicNameLastScaleUp  string
}

type apacheKafkaMetadata struct {
	BootstrapServers       []string          `keda:"name=bootstrapServers,       order=triggerMetadata;resolvedEnv"`
	Group                  string            `keda:"name=consumerGroup,          order=triggerMetadata;resolvedEnv"`
	Topic                  []string          `keda:"name=topic,                  order=triggerMetadata;resolvedEnv, optional"`
	PartitionLimitation    []int             `keda:"name=partitionLimitation,    order=triggerMetadata, optional, range"`
	LagThreshold           int64             `keda:"name=lagThreshold,           order=triggerMetadata, default=10"`
	ActivationLagThreshold int64             `keda:"name=activationLagThreshold, order=triggerMetadata, default=0"`
	OffsetResetPolicy      offsetResetPolicy `keda:"name=offsetResetPolicy,      order=triggerMetadata, enum=earliest;latest, default=latest"`
	AllowIdleConsumers     bool              `keda:"name=allowIdleConsumers,     order=triggerMetadata, optional"`
	ExcludePersistentLag   bool              `keda:"name=excludePersistentLag,   order=triggerMetadata, optional"`
	// Scale on ratio of lag to residual lag instead of lagThreshold.  Residual lag is calculated as published messages/s * CommitInterval / 2
	// Consumer group consuming a topic at the same rate as publisher will have a LagRatio of 1.0 on average and between 0.0 and 2.0.
	// A consumer group that is consuming messages 'CommitInterval' after published would have on average a LafRatio of 2.0
	// 3.0 would is a good starting value.
	LagRatio                  float64 `keda:"name=lagRatio,                  order=triggerMetadata, optional"`
	CommitInterval            int64   `keda:"name=commitInterval,            order=triggerMetadata, optional"`
	MinPartitionWriteThrouput float64 `keda:"name=minPartitionWriteThrouput, order=triggerMetadata, default=0.5"`
	MeasurementsForScale      int64   `keda:"name=measurementsForScale,      order=triggerMetadata, default=3"`
	ScaleDownFactor           float64 `keda:"name=scaleDownFactor,           order=triggerMetadata, default=0.75"`

	// If an invalid offset is found, whether to scale to 1 (false - the default) so consumption can
	// occur or scale to 0 (true). See discussion in https://github.com/kedacore/keda/issues/2612
	ScaleToZeroOnInvalidOffset bool `keda:"name=scaleToZeroOnInvalidOffset, order=triggerMetadata, optional"`
	LimitToPartitionsWithLag   bool `keda:"name=limitToPartitionsWithLag,   order=triggerMetadata, optional"`

	// SASL
	SASLType kafkaSaslType `keda:"name=sasl,     order=triggerMetadata;authParams, enum=none;plaintext;scram_sha256;scram_sha512;gssapi;aws_msk_iam, default=none"`
	Username string        `keda:"name=username, order=authParams,                 optional"`
	Password string        `keda:"name=password, order=authParams,                 optional"`

	// MSK
	AWSRegion        string `keda:"name=awsRegion,     order=triggerMetadata, optional"`
	AWSEndpoint      string `keda:"name=awsEndpoint,   order=triggerMetadata, optional"`
	AWSAuthorization awsutils.AuthorizationMetadata

	// TLS
	TLS         string `keda:"name=tls,         order=triggerMetadata;authParams, enum=enable;disable, default=disable"`
	Cert        string `keda:"name=cert,        order=authParams,                 optional"`
	Key         string `keda:"name=key,         order=authParams,                 optional"`
	KeyPassword string `keda:"name=keyPassword, order=authParams,                 optional"`
	CA          string `keda:"name=ca,          order=authParams,                 optional"`

	triggerIndex int
}

func (a *apacheKafkaMetadata) enableTLS() bool {
	return a.TLS == stringEnable
}

func (a *apacheKafkaMetadata) Validate() error {
	if a.LagThreshold <= 0 {
		return fmt.Errorf("lagThreshold must be a positive number")
	}
	if a.ActivationLagThreshold < 0 {
		return fmt.Errorf("activationLagThreshold must be a positive number")
	}
	if a.AllowIdleConsumers && a.LimitToPartitionsWithLag {
		return fmt.Errorf("allowIdleConsumers and limitToPartitionsWithLag cannot be set simultaneously")
	}
	// TODO: this works with the lag ration
	/*
		if len(a.Topic) == 0 && a.LimitToPartitionsWithLag {
			return fmt.Errorf("topic must be specified when using limitToPartitionsWithLag")
		}
	*/
	if len(a.Topic) == 0 && len(a.PartitionLimitation) > 0 {
		// no specific topics set, ignoring partitionLimitation setting
		a.PartitionLimitation = nil
	}
	if a.LagRatio != 0.0 && a.LagRatio < 1.0 {
		return fmt.Errorf("lagRatio must be a float greater than 1.0")
	}
	if a.CommitInterval < 0 {
		return fmt.Errorf("commitInterval must be a positive number")
	}
	if a.LagRatio != 0.0 && a.CommitInterval == 0 {
		return fmt.Errorf("commitInterval is required with lagRatio")
	}
	if a.enableTLS() && ((a.Cert == "") != (a.Key == "")) {
		return fmt.Errorf("can't set only one of cert or key when using TLS")
	}
	switch a.SASLType {
	case KafkaSASLTypePlaintext:
		if a.Username == "" || a.Password == "" {
			return fmt.Errorf("username and password must be set when using SASL/PLAINTEXT")
		}
	case KafkaSASLTypeMskIam:
		if a.AWSRegion == "" {
			return fmt.Errorf("awsRegion must be set when using AWS MSK IAM")
		}
		if !a.enableTLS() {
			return fmt.Errorf("TLS must be enabled when using AWS MSK IAM")
		}
	}
	return nil
}

const (
	KafkaSASLTypeMskIam = "aws_msk_iam"
)

// NewApacheKafkaScaler creates a new apacheKafkaScaler
func NewApacheKafkaScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	kafkaMetadata, err := parseApacheKafkaMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing kafka metadata: %w", err)
	}

	logger := InitializeLogger(config, "apache_kafka_scaler")
	client, err := getApacheKafkaClient(ctx, kafkaMetadata, logger)
	if err != nil {
		return nil, err
	}

	previousOffsets := make(map[string]map[int]int64)
	previousLastOffsets := make(map[string]map[int]int64)

	return &apacheKafkaScaler{
		client:              client,
		metricType:          metricType,
		metadata:            kafkaMetadata,
		logger:              logger,
		previousOffsets:     previousOffsets,
		previousLastOffsets: previousLastOffsets,
	}, nil
}

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

func getApacheKafkaClient(ctx context.Context, metadata apacheKafkaMetadata, logger logr.Logger) (*kafka.Client, error) {
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

func (s *apacheKafkaScaler) getTopicPartitions(ctx context.Context) (map[string][]int, error) {
	metadata, err := s.client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: s.client.Addr,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting metadata: %w", err)
	}
	s.logger.V(5).Info(fmt.Sprintf("Listed topics %v", metadata.Topics))

	if len(s.metadata.Topic) == 0 {
		// in case of empty topic name, we will get all topics that the consumer group is subscribed to
		describeGrpReq := &kafka.DescribeGroupsRequest{
			Addr: s.client.Addr,
			GroupIDs: []string{
				s.metadata.Group,
			},
			// TODO: does not seem to be helping with the version 3 responce, added to make response
			// compabible.
			IncludeAuthorizedOperations: 0,
		}

		describeGrp, err := s.client.DescribeGroups(ctx, describeGrpReq)

		if err != nil {
			return nil, fmt.Errorf("error describing group: %w", err)
		}
		if len(describeGrp.Groups[0].Members) == 0 {
			return nil, fmt.Errorf("no active members in group %s, group-state is %s", s.metadata.Group, describeGrp.Groups[0].GroupState)
		}
		s.logger.V(4).Info(fmt.Sprintf("Described group %s with response %v", s.metadata.Group, describeGrp))

		result := make(map[string][]int)

		topicsInGroup := describeGrp.Groups[0].Members[0].MemberMetadata.Topics

		for _, topic := range metadata.Topics {
			partitions := make([]int, 0)
			if kedautil.Contains(topicsInGroup, topic.Name) {
				s.logger.V(0).Info(fmt.Sprintf("YYYYY topic name: %s", topic.Name))
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
		return result, nil
	}
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
	return result, nil
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

/*
getLagForPartition returns (lag, lagWithPersistent, error)

When excludePersistentLag is set to `false` (default), lag will always be equal to lagWithPersistent
When excludePersistentLag is set to `true`, if partition is deemed to have persistent lag, lag will be set to 0 and lagWithPersistent will be latestOffset - consumerOffset
These return values will allow proper scaling from 0 -> 1 replicas by the IsActive func.
*/
func (s *apacheKafkaScaler) getLagForPartition(topic string, partitionID int, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (int64, int64, error) {
	if len(consumerOffsets) == 0 {
		return 0, 0, fmt.Errorf("consumerOffsets is empty")
	}
	if len(producerOffsets) == 0 {
		return 0, 0, fmt.Errorf("producerOffsets is empty")
	}

	consumerOffset := consumerOffsets[topic][partitionID]
	if consumerOffset == invalidOffset && s.metadata.OffsetResetPolicy == latest {
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

	if consumerOffset == invalidOffset && s.metadata.OffsetResetPolicy == earliest {
		if s.metadata.ScaleToZeroOnInvalidOffset {
			return 0, 0, nil
		}
		return producerOffset, producerOffset, nil
	}

	// This code block tries to prevent KEDA Kafka trigger from scaling the scale target based on erroneous events
	if s.metadata.ExcludePersistentLag {
		switch previousOffset, found := s.previousOffsets[topic][partitionID]; {
		case !found:
			// No record of previous offset, so store current consumer offset
			// Allow this consumer lag to be considered in scaling
			if _, topicFound := s.previousOffsets[topic]; !topicFound {
				s.previousOffsets[topic] = map[int]int64{partitionID: consumerOffset}
			} else {
				s.previousOffsets[topic][partitionID] = consumerOffset
			}
		case previousOffset == consumerOffset:
			// Indicates consumer is still on the same offset as the previous polling cycle, there may be some issue with consuming this offset.
			// return 0, so this consumer lag is not considered for scaling
			return 0, producerOffset - consumerOffset, nil
		default:
			// Successfully Consumed some messages, proceed to change the previous offset
			s.previousOffsets[topic][partitionID] = consumerOffset
		}
	}

	s.logger.V(4).Info(fmt.Sprintf("Consumer offset for topic %s in group %s and partition %d is %d", topic, s.metadata.Group, partitionID, consumerOffset))
	s.logger.V(4).Info(fmt.Sprintf("Producer offset for topic %s in group %s and partition %d is %d", topic, s.metadata.Group, partitionID, producerOffset))

	return producerOffset - consumerOffset, producerOffset - consumerOffset, nil
}

/*
getLagRatioForPartition returns (lagRatio, lagRatioWithPersistent, error)

# TODO excludePersistentLag

When excludePersistentLag is set to `false` (default), lag will always be equal to lagWithPersistent
When excludePersistentLag is set to `true`, if partition is deemed to have persistent lag, lagRatio
will be set to minLagRatio and lagRatioWithPersistent will be regular calculation
These return values will allow proper scaling from 0 -> 1 replicas by the IsActive func.
*/
func (s *apacheKafkaScaler) getLagRatioForPartition(topic string, partitionID int, now int64, consumerOffsets map[string]map[int]int64, producerOffsets map[string]map[int]int64) (float64, float64, error) {
	if len(consumerOffsets) == 0 {
		return 0, 0, fmt.Errorf("consumerOffsets is empty")
	}
	if len(producerOffsets) == 0 {
		return 0, 0, fmt.Errorf("producerOffsets is empty")
	}

	consumerOffset := consumerOffsets[topic][partitionID]
	if consumerOffset == invalidOffset {
		if s.metadata.ScaleToZeroOnInvalidOffset {
			// TODO - do not use initially
		}
		msg := fmt.Sprintf(
			"Kafka lagRatio, invalid committed offset found for topic %s in group %s and partition %d, probably no offset is committed yet. Returning with lag ratio of %.6f",
			topic, s.metadata.Group, partitionID, 0.0)
		s.logger.V(1).Info(msg)
		return 0.0, 0.0, nil
	}

	if _, found := producerOffsets[topic]; !found {
		return 0, 0, fmt.Errorf("error finding partition producer offset for topic %s", topic)
	}
	producerOffset := producerOffsets[topic][partitionID]
	previousProducerOffset, found := s.previousLastOffsets[topic][partitionID]
	previousLastOffsettime := s.lastOffetsTime

	s.logger.V(3).Info(fmt.Sprintf("Kafka lagRatio, offsets for group %s topic partition %s:%d, producerOffset %d, previousProducerOffset %d, previousLastOffsettime %d", s.metadata.Group, topic, partitionID, producerOffset, previousProducerOffset, previousLastOffsettime))

	switch {
	case !found:
		// No record of previous last offset, so store current consumer offset
		if _, topicFound := s.previousLastOffsets[topic]; !topicFound {
			s.previousLastOffsets[topic] = map[int]int64{partitionID: producerOffset}
		} else {
			s.previousLastOffsets[topic][partitionID] = producerOffset
		}
		s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, first get metrics call for topic partion %s:%d, scaler needs a previous last offset value, returning 0.0", topic, partitionID))
		return 0.0, 0.0, nil
	case previousProducerOffset == producerOffset:
		s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio, no writes on topic partition %s:%d since last get metrics call, returning 0.0", topic, partitionID))
		return 0.0, 0.0, nil
	default:
		s.previousLastOffsets[topic][partitionID] = producerOffset
	}

	// number of messages written to the partition since last run
	messages := producerOffset - previousProducerOffset
	if messages < 0 {
		return 0, 0, fmt.Errorf("lagRatio, unexpected error calculating messages/s for topic partition %s:%d", topic, partitionID)
	}
	// in messages per milliseconds
	period := now - previousLastOffsettime
	if period <= 0 {
		return 0, 0, fmt.Errorf("lagRatio, unexpected error calculating period for topic partition %s:%d", topic, partitionID)
	}
	ratio := 0.0
	residualLag := 0.0
	writeThroughput := float64(messages) / float64(period)
	s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio Write througput %.3f messages/s for last %.3f seconds for topic partion %s:%d", writeThroughput*1000, float64(period)/1000, topic, partitionID))

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

	return ratio, writeThroughput, nil
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

// GetMetricsAndActivity returns value for a supported metric and an error if there is a problem getting the metric
func (s *apacheKafkaScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	if s.metadata.LagRatio != 0.0 {
		toplLagRatio, _, err := s.getTotalLagRatio(ctx)
		if err != nil {
			s.logger.V(5).Info(fmt.Sprintf("GetMetricsAndActivity err %s", err))
			return []external_metrics.ExternalMetricValue{}, false, err
		} else {
			s.logger.V(0).Info(fmt.Sprintf("GetMetricsAndActivity topLagRatio %f", toplLagRatio))
		}

		metric := GenerateMetricInMili(metricName, toplLagRatio)
		return []external_metrics.ExternalMetricValue{metric}, true, nil

	} else {
		totalLag, totalLagWithPersistent, err := s.getTotalLag(ctx)
		if err != nil {
			return []external_metrics.ExternalMetricValue{}, false, err
		}
		metric := GenerateMetricInMili(metricName, float64(totalLag))
		return []external_metrics.ExternalMetricValue{metric}, totalLagWithPersistent > s.metadata.ActivationLagThreshold, nil
	}
}

// getTotalLag returns totalLag, totalLagWithPersistent, error
// totalLag and totalLagWithPersistent are the summations of lag and lagWithPersistent returned by getLagForPartition function respectively.
// totalLag maybe less than totalLagWithPersistent when excludePersistentLag is set to `true` due to some partitions deemed as having persistent lag
func (s *apacheKafkaScaler) getTotalLag(ctx context.Context) (int64, int64, error) {
	topicPartitions, err := s.getTopicPartitions(ctx)
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

	topicPartitions, err := s.getTopicPartitions(ctx)
	if err != nil {
		return 0, 0, err
	}
	consumerOffsets, producerOffsets, err := s.getConsumerAndProducerOffsets(ctx, topicPartitions)
	s.logger.V(5).Info(fmt.Sprintf("Kafka scaler: Consumer offsets %v, producer offsets %v", consumerOffsets, producerOffsets))
	if err != nil {
		return 0, 0, err
	}

	averageLagRatio := float64(0)
	topicWriteThroughput := float64(0)
	totalTopicPartitions := int64(0)
	partitionsWithLag := int64(0)
	//
	topicLargestRatio := float64(0)
	topicNameLargestRatio := ""
	topiclargestWriteThroughput := float64(0)

	now := time.Now().UnixNano() / int64(time.Millisecond)
	for topic, partitionsOffsets := range producerOffsets {
		// used to record approximate period since last metrics check to calculate per partition write throughout

		averageLagRatio = 0
		for partition := range partitionsOffsets {
			lagRatio, writeThroughput, err := s.getLagRatioForPartition(topic, partition, now, consumerOffsets, producerOffsets)
			if err != nil {
				return 0.0, 0.0, err
			}
			averageLagRatio += lagRatio
			topicWriteThroughput += writeThroughput
			if lagRatio > 0 {
				partitionsWithLag++
			}
		}
		totalTopicPartitions = (int64)(len(partitionsOffsets))
		if s.metadata.LimitToPartitionsWithLag {
			if partitionsWithLag == 0 {
				averageLagRatio = 0
			} else {
				averageLagRatio /= float64(partitionsWithLag)
			}
			// TODO temporary level 0
			s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, lagRatio with only %d partitions with lag: %.6f for group: %s, topic: %s", partitionsWithLag, averageLagRatio, s.metadata.Group, topic))
		} else {
			averageLagRatio /= float64(totalTopicPartitions)
			s.logger.V(2).Info(fmt.Sprintf("Kafka lagRatio, lagRatio average across all partitions: %.6f for group: %s, topic: %s", averageLagRatio, s.metadata.Group, topic))
		}

		if averageLagRatio > topicLargestRatio {
			topicNameLargestRatio = topic
			topicLargestRatio = averageLagRatio
			topiclargestWriteThroughput = topicWriteThroughput
		}

		// Scaling down when write throughput goes down 'enough' from where it was in the scale up.
		if topic == s.topicNameLastScaleUp {
			// XXX TODO temporary >= 0 - needed to test scale down
			if topicWriteThroughput >= 0 && topicWriteThroughput < s.wThrougoutLastScaleUp*s.metadata.ScaleDownFactor {
				s.thresholdCountDown++
			} else {
				s.thresholdCountDown = 0
			}
		}
	}
	s.lastOffetsTime = now

	if topicNameLargestRatio != "" {
		s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, largest ratio %.6f for group: %s is in topic %s, threshold %.6f", averageLagRatio, s.metadata.Group, topicNameLargestRatio, s.metadata.LagRatio))
	} else {
		s.logger.V(1).Info(fmt.Sprintf("Kafka lagRatio, group: %s has no topic with lagRatio > 0.0", s.metadata.Group))
	}

	cappedLogRatio := topicLargestRatio
	if cappedLogRatio > s.metadata.LagRatio {
		// above scale up threshold, reset scale down.
		s.thresholdCountDown = 0

		if s.thresholdCountUp++; s.thresholdCountUp >= s.metadata.MeasurementsForScale {
			// reset so we return a metric to HPA above targer only once
			s.thresholdCountUp = 0
			// When we cross the lagRatio threshold N times, return a metric to HPA
			// just large enough to add 1 replica at a time for approximatively each ten replicas.
			// TODO: use HPA policies instead
			cappedLogRatio = 2.0 * s.metadata.LagRatio

			// scale up event, record topic write through put.
			s.topicNameLastScaleUp = topicNameLargestRatio
			s.wThrougoutLastScaleUp = topiclargestWriteThroughput
			s.logger.V(0).Info(fmt.Sprintf("Kafka lagRatio, Recording write throuput of %f on scale up for group: %s is in topic %s", s.wThrougoutLastScaleUp, s.metadata.Group, topicNameLargestRatio))
		} else {
			// we crossed threshold, but not number of measurements requried, just report metric as target
			// this will not cause any scaling.
			cappedLogRatio = s.metadata.LagRatio
		}
	} else {
		// below scale down threshold, reset scale up
		s.thresholdCountUp = 0
		if s.thresholdCountDown >= s.metadata.MeasurementsForScale {
			s.thresholdCountDown = 0
			cappedLogRatio = topicWriteThroughput / s.wThrougoutLastScaleUp

			s.logger.V(0).Info(fmt.Sprintf("Kafka lagRatio, scaling down group: %s on topic %s", s.metadata.Group, topicNameLargestRatio))
		} else {
			cappedLogRatio = s.metadata.LagRatio
		}
	}

	/*
		if cappedLogRatio == 0 {
			s.logger.V(0).Info("XXX PATCHED CAPPED VALUE from 0")
			cappedLogRatio = s.metadata.LagRatio
		}
	*/

	s.logger.V(0).Info(fmt.Sprintf("HPA Metric: %f, lag ratio: %f, throughput: %f, Recoreded scale down topic/throughput: %s/%f, counts up/down: %d/%d",
		cappedLogRatio, topicLargestRatio, topicWriteThroughput*1000, s.topicNameLastScaleUp, s.wThrougoutLastScaleUp*1000, s.thresholdCountUp, s.thresholdCountDown))

	return cappedLogRatio, cappedLogRatio, nil
}
