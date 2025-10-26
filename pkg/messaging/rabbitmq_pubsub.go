package messaging

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqPubSub struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
}

type rabbitmqSubscription struct {
	channel     *amqp.Channel
	queue       string
	consumerTag string
}

func (s *rabbitmqSubscription) Unsubscribe() error {
	if s.consumerTag != "" {
		return s.channel.Cancel(s.consumerTag, false)
	}
	return nil
}

func NewRabbitMQPubSub(conn *amqp.Connection) (PubSub, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare exchange once during initialization
	err = ch.ExchangeDeclare(
		"mpc_pubsub",
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &rabbitmqPubSub{conn: conn, channel: ch}, nil
}

func (r *rabbitmqPubSub) Publish(topic string, message []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.channel.Publish(
		"mpc_pubsub",
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

func (r *rabbitmqPubSub) PublishWithReply(topic, reply string, data []byte, headers map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	amqpHeaders := amqp.Table{}
	for k, v := range headers {
		amqpHeaders[k] = v
	}

	return r.channel.Publish(
		"mpc_pubsub",
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         data,
			ReplyTo:      reply,
			Headers:      amqpHeaders,
			DeliveryMode: amqp.Persistent,
		},
	)
}

func (r *rabbitmqPubSub) Subscribe(topic string, handler func(msg *nats.Msg)) (Subscription, error) {
	q, err := r.channel.QueueDeclare(
		"",    // auto-generated name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	err = r.channel.QueueBind(
		q.Name,
		topic,
		"mpc_pubsub",
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}

	consumerTag := fmt.Sprintf("consumer-%s-%d", q.Name, time.Now().UnixNano())

	msgs, err := r.channel.Consume(
		q.Name,
		consumerTag,
		false, // manual ack for reliability
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
			// Convert RabbitMQ headers to NATS format
			natsHeaders := nats.Header{}
			for k, v := range d.Headers {
				if str, ok := v.(string); ok {
					natsHeaders.Set(k, str)
				}
			}

			handler(&nats.Msg{
				Subject: topic,
				Reply:   d.ReplyTo,
				Data:    d.Body,
				Header:  natsHeaders,
			})

			d.Ack(false)
		}
	}()

	return &rabbitmqSubscription{
		channel:     r.channel,
		queue:       q.Name,
		consumerTag: consumerTag,
	}, nil
}
