package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fystack/mpcium/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrInvalidStreamName    = errors.New("stream name cannot be empty")
	ErrInvalidSubjects      = errors.New("subjects cannot be empty")
	ErrConsumerCreation     = errors.New("failed to create consumer")
	ErrStreamCreation       = errors.New("failed to create stream")
	ErrInvalidConfiguration = errors.New("invalid configuration")
	ErrConnectionClosed     = errors.New("connection is closed")
	ErrSubscriptionClosed   = errors.New("subscription is closed")
)

const (
	DefaultAckWait             = 30 * time.Second
	DefaultMaxDeliveryAttempts = 3
	DefaultConsumerPrefix      = "consumer"
	DefaultStreamMaxAge        = 3 * time.Minute
	DefaultBackoffDuration     = 30 * time.Second
)

// MessageBroker interface - updated to use custom Message type
type MessageBroker interface {
	PublishMessage(ctx context.Context, subject string, data []byte) error
	CreateSubscription(ctx context.Context, consumerName, subject string, handler func(msg Message)) (MessageSubscription, error)
	GetStreamInfo(ctx context.Context) (*StreamInfo, error)
	FetchMessages(ctx context.Context, consumerName, subject string, batchSize int, handler func(msg Message)) error
	Close() error
}

type MessageSubscription interface {
	Unsubscribe() error
}

type BrokerOption func(*brokerConfiguration)

type brokerConfiguration struct {
	streamName          string
	subjects            []string
	description         string
	maxAge              time.Duration
	ackWait             time.Duration
	maxDeliveryAttempts int
	consumerNamePrefix  string
	backoffDurations    []time.Duration
}

type rabbitmqBroker struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	subjects     []string
	config       brokerConfiguration
}

// Broker option functions
func WithDescription(description string) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.description = description
	}
}

func WithMaxAge(maxAge time.Duration) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.maxAge = maxAge
	}
}

func WithAckWait(ackWait time.Duration) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.ackWait = ackWait
	}
}

func WithMaxDeliveryAttempts(maxAttempts int) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.maxDeliveryAttempts = maxAttempts
	}
}

func WithConsumerNamePrefix(prefix string) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.consumerNamePrefix = prefix
	}
}

func WithBackoffDurations(durations []time.Duration) BrokerOption {
	return func(cfg *brokerConfiguration) {
		cfg.backoffDurations = durations
	}
}

func NewRabbitMQBroker(
	ctx context.Context,
	conn *amqp.Connection,
	streamName string,
	subjects []string,
	opts ...BrokerOption,
) (MessageBroker, error) {
	if streamName == "" {
		return nil, ErrInvalidStreamName
	}
	if len(subjects) == 0 {
		return nil, ErrInvalidSubjects
	}

	config := brokerConfiguration{
		streamName:          streamName,
		subjects:            subjects,
		description:         fmt.Sprintf("Stream for %s", streamName),
		maxAge:              DefaultStreamMaxAge,
		ackWait:             DefaultAckWait,
		maxDeliveryAttempts: DefaultMaxDeliveryAttempts,
		consumerNamePrefix:  DefaultConsumerPrefix,
		backoffDurations:    []time.Duration{DefaultBackoffDuration, DefaultBackoffDuration, DefaultBackoffDuration},
	}

	for _, opt := range opts {
		opt(&config)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare topic exchange for the stream
	err = ch.ExchangeDeclare(
		streamName,
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w for exchange '%s': %v", ErrStreamCreation, streamName, err)
	}

	// Declare dead letter exchange for failed messages
	dlxName := fmt.Sprintf("%s_dlx", streamName)
	err = ch.ExchangeDeclare(
		dlxName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLX for stream '%s': %w", streamName, err)
	}

	broker := &rabbitmqBroker{
		conn:         conn,
		channel:      ch,
		exchangeName: streamName,
		subjects:     subjects,
		config:       config,
	}

	logger.Info("RabbitMQ broker initialized successfully", "exchange", streamName)
	return broker, nil
}

func (b *rabbitmqBroker) PublishMessage(ctx context.Context, subject string, data []byte) error {
	if b.conn.IsClosed() {
		return ErrConnectionClosed
	}

	err := b.channel.Publish(
		b.exchangeName,
		subject,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         data,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to subject %s: %w", subject, err)
	}

	return nil
}

func (b *rabbitmqBroker) CreateSubscription(
	ctx context.Context,
	consumerName, subject string,
	handler func(msg Message),
) (MessageSubscription, error) {
	// Create a NEW dedicated channel for this subscription
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Set QoS for this channel
	err = ch.Qos(1, 0, false)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	sanitizedName := sanitizeConsumerName(consumerName)

	// Declare queue
	queueName := fmt.Sprintf("%s_queue", sanitizedName)
	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(queueName, subject, b.exchangeName, false, nil)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}

	// Start consuming
	msgs, err := ch.Consume(
		queueName,
		sanitizedName,
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return nil, fmt.Errorf("failed to consume: %w", err)
	}

	stopChan := make(chan struct{})
	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return
				}
				msg := &rabbitmqMessage{
					delivery: d,
					subject:  subject,
				}
				handler(msg)
			case <-stopChan:
				return
			}
		}
	}()

	return &rabbitmqBrokerSubscription{
		channel:      ch,
		queueName:    queueName,
		consumerName: sanitizedName,
		stopChan:     stopChan,
	}, nil
}

func (b *rabbitmqBroker) GetStreamInfo(ctx context.Context) (*StreamInfo, error) {
	// RabbitMQ doesn't have direct stream info equivalent
	// Return minimal info structure
	return &StreamInfo{
		Config: StreamConfig{
			Name:        b.exchangeName,
			Subjects:    b.subjects,
			Description: b.config.description,
		},
		State: StreamState{
			Messages:  0,
			Bytes:     0,
			FirstSeq:  0,
			LastSeq:   0,
			Consumers: 0,
		},
	}, nil
}

func (b *rabbitmqBroker) FetchMessages(
	ctx context.Context,
	consumerName, subject string,
	batchSize int,
	handler func(msg Message),
) error {
	if b.conn.IsClosed() {
		return ErrConnectionClosed
	}

	sanitizedName := sanitizeConsumerName(consumerName)
	queueName := fmt.Sprintf("%s_%s", b.exchangeName, sanitizedName)

	ch, err := b.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer ch.Close()

	// Fetch messages in batch
	for i := 0; i < batchSize; i++ {
		delivery, ok, err := ch.Get(queueName, false)
		if err != nil {
			return fmt.Errorf("failed to fetch message: %w", err)
		}
		if !ok {
			break // No more messages
		}

		msg := &rabbitmqMessage{
			delivery: delivery,
			subject:  subject,
		}
		handler(msg)
	}

	logger.Info("Fetched messages successfully", "consumer", sanitizedName, "subject", subject, "batchSize", batchSize)
	return nil
}

func (b *rabbitmqBroker) Close() error {
	if b.channel != nil {
		return b.channel.Close()
	}
	return nil
}

// rabbitmqBrokerSubscription implements MessageSubscription
type rabbitmqBrokerSubscription struct {
	channel      *amqp.Channel
	queueName    string
	consumerName string
	stopChan     chan struct{}
}

// func (s *rabbitmqBrokerSubscription) Unsubscribe() error {
// 	close(s.stopChan)
// 	if s.channel != nil {
// 		s.channel.Cancel(s.consumerName, false)
// 		return s.channel.Close()
// 	}
// 	return nil
// }

func (s *rabbitmqBrokerSubscription) Unsubscribe() error {
	// Cancel the consumer first
	if s.channel != nil && !s.channel.IsClosed() {
		if err := s.channel.Cancel(s.consumerName, false); err != nil {
			// Log but don't fail - channel might already be closed
			logger.Error("Failed to cancel consumer", err)
		}
	}

	// Signal the goroutine to stop
	select {
	case <-s.stopChan:
		// Already closed
	default:
		close(s.stopChan)
	}

	// Close the channel last
	if s.channel != nil && !s.channel.IsClosed() {
		return s.channel.Close()
	}

	return nil
}

// rabbitmqMessage adapts RabbitMQ Delivery to custom Message interface
type rabbitmqMessage struct {
	delivery amqp.Delivery
	subject  string
}

func (m *rabbitmqMessage) Data() []byte {
	return m.delivery.Body
}

func (m *rabbitmqMessage) Headers() map[string][]string {
	headers := make(map[string][]string)
	for k, v := range m.delivery.Headers {
		if str, ok := v.(string); ok {
			headers[k] = []string{str}
		}
	}
	return headers
}

func (m *rabbitmqMessage) Subject() string {
	return m.subject
}

func (m *rabbitmqMessage) Ack() error {
	return m.delivery.Ack(false)
}

func (m *rabbitmqMessage) Nak() error {
	return m.delivery.Nack(false, true)
}

func (m *rabbitmqMessage) Term() error {
	return m.delivery.Reject(false)
}

func (m *rabbitmqMessage) InProgress() error {
	// RabbitMQ doesn't have direct equivalent
	return nil
}

func (m *rabbitmqMessage) Metadata() (*MessageMetadata, error) {
	return &MessageMetadata{
		Sequence: SequencePair{
			Consumer: uint64(m.delivery.DeliveryTag),
			Stream:   0,
		},
		NumDelivered: int(m.delivery.DeliveryTag),
		Timestamp:    m.delivery.Timestamp,
	}, nil
}
