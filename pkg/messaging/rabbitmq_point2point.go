package messaging

import (
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqDirectMessaging struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	handlers map[string][]func([]byte)
	mu       sync.Mutex
}

func NewRabbitMQDirectMessaging(conn *amqp.Connection) (DirectMessaging, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare direct exchange
	err = ch.ExchangeDeclare(
		"mpc_direct",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &rabbitmqDirectMessaging{
		conn:     conn,
		channel:  ch,
		handlers: make(map[string][]func([]byte)),
	}, nil
}

func (r *rabbitmqDirectMessaging) SendToOtherWithRetry(topic string, message []byte, config RetryConfig) error {
	opts := []retry.Option{
		retry.MaxJitter(80 * time.Millisecond),
	}

	if config.RetryAttempt > 0 {
		opts = append(opts, retry.Attempts(config.RetryAttempt))
	}
	if config.ExponentialBackoff {
		opts = append(opts, retry.DelayType(retry.BackOffDelay))
	}
	if config.Delay > 0 {
		opts = append(opts, retry.Delay(config.Delay))
	}
	if config.OnRetry != nil {
		opts = append(opts, retry.OnRetry(config.OnRetry))
	}

	return retry.Do(
		func() error {
			return r.SendToOther(topic, message)
		},
		opts...,
	)
}

func (r *rabbitmqDirectMessaging) SendToSelf(topic string, message []byte) error {
	r.mu.Lock()
	handlers := r.handlers[topic]
	r.mu.Unlock()

	for _, handler := range handlers {
		go handler(message)
	}
	return nil
}

func (r *rabbitmqDirectMessaging) SendToOther(topic string, message []byte) error {
	return r.channel.Publish(
		"mpc_direct",
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		},
	)
}

func (r *rabbitmqDirectMessaging) Listen(topic string, handler func(data []byte)) (Subscription, error) {
	q, err := r.channel.QueueDeclare(
		topic,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	err = r.channel.QueueBind(
		q.Name,
		topic,
		"mpc_direct",
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}

	consumerTag := fmt.Sprintf("direct-%s-%d", topic, time.Now().UnixNano())

	msgs, err := r.channel.Consume(
		q.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to consume: %w", err)
	}

	go func() {
		for d := range msgs {
			handler(d.Body)

			// Send reply if ReplyTo is set
			if d.ReplyTo != "" {
				r.channel.Publish(
					"",
					d.ReplyTo,
					false,
					false,
					amqp.Publishing{
						ContentType:   "application/octet-stream",
						CorrelationId: d.CorrelationId,
						Body:          []byte("OK"),
					},
				)
			}
			d.Ack(false)
		}
	}()

	r.mu.Lock()
	r.handlers[topic] = append(r.handlers[topic], handler)
	r.mu.Unlock()

	return &rabbitmqSubscription{
		channel:     r.channel,
		queue:       q.Name,
		consumerTag: consumerTag,
	}, nil
}
