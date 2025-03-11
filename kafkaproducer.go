package kafkaproducer

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/johandrevandeventer/kafkaproducer/config"
	"github.com/johandrevandeventer/kafkaproducer/payload"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Configurable parameters
const (
	defaultTimeout = 10 * time.Second // Default timeout for sending messages
	maxRetries     = 5                // Maximum number of retries for sending messages
)

// Metrics
var (
	messagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_messages_produced_total",
		Help: "Total number of messages produced to Kafka",
	}, []string{"topic"})

	deliveryErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_delivery_errors_total",
		Help: "Total number of message delivery errors",
	}, []string{"topic"})
)

type KafkaProducer struct {
	producer     *kafka.Producer
	topic        string
	logger       *zap.Logger
	deliveryChan chan kafka.Event
	wg           sync.WaitGroup
}

func NewKafkaProducer(cfg *config.KafkaConfig, logger *zap.Logger) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
	})
	if err != nil {
		return nil, err
	}

	logger.Info("Kafka producer created successfully")

	kp := &KafkaProducer{
		producer:     p,
		topic:        cfg.Topic,
		logger:       logger,
		deliveryChan: make(chan kafka.Event, 10000),
	}

	// Start handling delivery reports
	kp.wg.Add(1)
	go kp.handleDeliveryReports()

	return kp, nil
}

// handleDeliveryReports processes delivery reports from Kafka.
func (kp *KafkaProducer) handleDeliveryReports() {
	defer kp.wg.Done()
	for e := range kp.deliveryChan {
		kp.processDeliveryEvent(e)
	}
}

// processDeliveryEvent handles Kafka message delivery results.
func (kp *KafkaProducer) processDeliveryEvent(e kafka.Event) {
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			kp.logger.Error("Failed to deliver message",
				zap.String("kafka_topic", *ev.TopicPartition.Topic),
				zap.Error(ev.TopicPartition.Error),
			)
			deliveryErrors.WithLabelValues(*ev.TopicPartition.Topic).Inc()
			return
		}

		p, err := payload.Deserialize(ev.Value)
		if err != nil {
			kp.logger.Error("Failed to deserialize payload",
				zap.Error(err),
			)
			return
		}

		kp.logger.Info("Message delivered",
			zap.String("kafka_topic", *ev.TopicPartition.Topic),
			zap.String("mqtt_topic", p.MqttTopic),
			zap.Int64("offset", int64(ev.TopicPartition.Offset)),
		)
		messagesProduced.WithLabelValues(*ev.TopicPartition.Topic).Inc()
	}
}

// SendMessage sends a message to Kafka with retries.
func (kp *KafkaProducer) SendMessage(ctx context.Context, message []byte) error {
	// Deserialize the incoming message into a Payload struct
	p, err := payload.Deserialize(message)
	if err != nil {
		kp.logger.Error("Failed to deserialize payload",
			zap.Error(err),
		)
		return err // Early return if deserialization fails
	}

	// Serialize the Payload struct into a byte slice (e.g., JSON)
	serializedPayload, err := p.Serialize()
	if err != nil {
		kp.logger.Error("Failed to serialize payload",
			zap.Error(err),
		)
		return err
	}

	operation := func() error {
		return kp.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny},
			Value:          serializedPayload,
		}, kp.deliveryChan)
	}

	retryBackoff := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries)
	err = backoff.RetryNotify(operation, backoff.WithContext(retryBackoff, ctx), func(err error, duration time.Duration) {
		kp.logger.Warn("Failed to send message, retrying...",
			zap.Error(err),
			zap.Duration("retry_after", duration),
		)
	})

	if err != nil {
		deliveryErrors.WithLabelValues(kp.topic).Inc()
		return err
	}

	return nil
}

// Close shuts down the Kafka producer gracefully.
func (kp *KafkaProducer) Close() {
	kp.logger.Info("Closing Kafka producer...")

	// Flush pending messages
	remaining := kp.producer.Flush(5000) // 5-second timeout
	if remaining > 0 {
		kp.logger.Warn("Failed to flush all messages", zap.Int("remaining_messages", remaining))
	} else {
		kp.logger.Info("All messages flushed successfully")
	}

	close(kp.deliveryChan)
	kp.producer.Close()
	kp.wg.Wait()
}
