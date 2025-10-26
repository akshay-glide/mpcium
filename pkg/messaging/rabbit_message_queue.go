package messaging

import (
	"errors"
	"fmt"

	"github.com/akshay-glide/mpcium/pkg/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqMessageQueue struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string // ADD THIS FIELD
	consumerName string
	queueName    string
}

type RabbitMQMessageQueueManager struct {
	conn             *amqp.Connection
	exchangeName     string
	subjectWildCards []string
}

func NewRabbitMQMessageQueueManager(exchangeName string, subjectWildCards []string, conn *amqp.Connection) *RabbitMQMessageQueueManager {
	ch, err := conn.Channel()
	if err != nil {
		logger.Fatal("Error creating RabbitMQ channel", err)
	}

	// Declare exchange for work queue pattern
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Fatal("Error creating RabbitMQ exchange", err)
	}

	ch.Close()

	return &RabbitMQMessageQueueManager{
		conn:             conn,
		exchangeName:     exchangeName,
		subjectWildCards: subjectWildCards,
	}
}

func (m *RabbitMQMessageQueueManager) NewMessageQueue(consumerName string) MessageQueue {
	ch, err := m.conn.Channel()
	if err != nil {
		logger.Fatal("Error creating channel for message queue", err)
	}

	// Set QoS to process one message at a time
	err = ch.Qos(1, 0, false)
	if err != nil {
		logger.Fatal("Error setting QoS", err)
	}

	// Declare durable queue with dead letter exchange
	queueName := fmt.Sprintf("%s_queue", consumerName)
	args := amqp.Table{
		"x-dead-letter-exchange": fmt.Sprintf("%s_dlx", m.exchangeName),
		"x-message-ttl":          int32(300000), // 5 minutes
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		logger.Fatal("Error declaring queue", err)
	}

	// Bind queue to all wildcard patterns
	for _, pattern := range m.subjectWildCards {
		err = ch.QueueBind(
			queueName,
			pattern,
			m.exchangeName,
			false,
			nil,
		)
		if err != nil {
			logger.Fatal("Error binding queue", err)
		}
	}

	return &rabbitmqMessageQueue{
		conn:         m.conn,
		channel:      ch,
		exchangeName: m.exchangeName, // PASS THE EXCHANGE NAME HERE
		consumerName: consumerName,
		queueName:    queueName,
	}
}

func (mq *rabbitmqMessageQueue) Enqueue(topic string, message []byte, options *EnqueueOptions) error {
	headers := amqp.Table{}
	if options != nil && options.IdempotententKey != "" {
		headers["x-deduplication-header"] = options.IdempotententKey
	}

	logger.Info("Publishing message", "topic", topic, "consumerName", mq.consumerName)

	err := mq.channel.Publish(
		mq.exchangeName, // exchange
		topic,           // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         message,
			DeliveryMode: amqp.Persistent, // persistent messages
			Headers:      headers,
		},
	)

	if err != nil {
		logger.Error("Failed to publish message to RabbitMQ", err, "topic", topic, "consumerName", mq.consumerName)
		return fmt.Errorf("error enqueueing message: %w", err)
	}

	return nil
}

func (mq *rabbitmqMessageQueue) Dequeue(topic string, handler func(message []byte) error) error {
	// Bind queue to topic
	err := mq.channel.QueueBind(
		mq.queueName,
		topic,
		mq.exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	msgs, err := mq.channel.Consume(
		mq.queueName,
		"",
		false, // manual ack for reliability
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume: %w", err)
	}

	go func() {
		for d := range msgs {
			err := handler(d.Body)
			if err != nil {
				if errors.Is(err, ErrPermament) {
					// Permanent error - reject without requeue
					logger.Info("Permanent error on message", "deliveryTag", d.DeliveryTag)
					if err := d.Reject(false); err != nil {
						logger.Error("Failed to reject message", err)
					}
					continue
				}

				// Transient error - nack with requeue
				logger.Error("Error handling message", err)
				if err := d.Nack(false, true); err != nil {
					logger.Error("Failed to nack message", err)
				}
				continue
			}

			// Success - acknowledge
			logger.Debug("Message acknowledged", "deliveryTag", d.DeliveryTag)
			if err := d.Ack(false); err != nil {
				logger.Error("Error acknowledging message", err)
			}
		}
	}()

	return nil
}

func (mq *rabbitmqMessageQueue) Close() {
	if mq.channel != nil {
		mq.channel.Close()
	}
}
