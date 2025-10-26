package eventconsumer

import (
	"encoding/json"
	"fmt"

	"github.com/fystack/mpcium/pkg/event"
	"github.com/fystack/mpcium/pkg/logger"
	"github.com/fystack/mpcium/pkg/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ uses dead letter queues instead of advisory messages
const deadLetterQueueName = "mpc_signing_dlq"

type timeOutConsumer struct {
	rabbitConn  *amqp.Connection
	resultQueue messaging.MessageQueue
	dlqSub      messaging.MessageSubscription
}

func NewTimeOutConsumer(rabbitConn *amqp.Connection, resultQueue messaging.MessageQueue) *timeOutConsumer {
	return &timeOutConsumer{
		rabbitConn:  rabbitConn,
		resultQueue: resultQueue,
	}
}

func (tc *timeOutConsumer) Run() {
	logger.Info("Starting dead letter queue consumer for timeout handling")

	ch, err := tc.rabbitConn.Channel()
	if err != nil {
		logger.Error("Failed to create channel for DLQ consumer", err)
		return
	}

	// Declare dead letter exchange
	dlxName := fmt.Sprintf("%s_dlx", event.SigningPublisherStream)
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
		logger.Error("Failed to declare DLX", err)
		return
	}

	// Declare dead letter queue
	dlqName := fmt.Sprintf("%s_dlq", event.SigningPublisherStream)
	_, err = ch.QueueDeclare(
		dlqName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to declare DLQ", err)
		return
	}

	// Bind DLQ to DLX
	err = ch.QueueBind(
		dlqName,
		"#", // catch all routing keys
		dlxName,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to bind DLQ to DLX", err)
		return
	}

	// Start consuming from dead letter queue
	msgs, err := ch.Consume(
		dlqName,
		"timeout_consumer",
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("Failed to consume from DLQ", err)
		return
	}

	go func() {
		for d := range msgs {
			logger.Info("Received message from DLQ", "deliveryTag", d.DeliveryTag)

			var signErrorResult event.SigningResultEvent
			err := json.Unmarshal(d.Body, &signErrorResult)
			if err != nil {
				logger.Error("Failed to unmarshal signing result event from DLQ", err)
				d.Ack(false)
				continue
			}

			// Mark as timeout error
			signErrorResult.ResultType = event.ResultTypeError
			signErrorResult.ErrorCode = event.ErrorCodeMaxDeliveryAttempts
			signErrorResult.IsTimeout = true
			signErrorResult.ErrorReason = "Signing failed: maximum delivery attempts exceeded"

			signErrorResultBytes, err := json.Marshal(signErrorResult)
			if err != nil {
				logger.Error("Failed to marshal signing result event", err)
				d.Ack(false)
				continue
			}

			err = tc.resultQueue.Enqueue(event.SigningResultTopic, signErrorResultBytes, &messaging.EnqueueOptions{
				IdempotententKey: signErrorResult.TxID,
			})
			if err != nil {
				logger.Error("Failed to publish signing result event for timeout", err)
				d.Nack(false, true) // requeue
				continue
			}

			logger.Info("Published signing result event for timeout", "txID", signErrorResult.TxID)
			d.Ack(false)
		}
	}()
}

func (tc *timeOutConsumer) Close() error {
	if tc.dlqSub != nil {
		return tc.dlqSub.Unsubscribe()
	}
	return nil
}
