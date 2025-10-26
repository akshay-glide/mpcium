package eventconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fystack/mpcium/pkg/event"
	"github.com/fystack/mpcium/pkg/logger"
	"github.com/fystack/mpcium/pkg/messaging"
	"github.com/fystack/mpcium/pkg/mpc"
	"github.com/fystack/mpcium/pkg/types"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Maximum time to wait for a signing response.
	keygenResponseTimeout = 30 * time.Second
	// How often to poll for the reply message.
	keygenPollingInterval = 500 * time.Millisecond
)

// KeygenConsumer represents a consumer that processes signing events.
type KeygenConsumer interface {
	// Run starts the consumer and blocks until the provided context is canceled.
	Run(ctx context.Context) error
	// Close performs a graceful shutdown of the consumer.
	Close() error
}

// keygenConsumer implements KeygenConsumer.
type keygenConsumer struct {
	rabbitConn        *amqp.Connection
	pubsub            messaging.PubSub
	jsBroker          messaging.MessageBroker
	peerRegistry      mpc.PeerRegistry
	keygenResultQueue messaging.MessageQueue
	jsSub             messaging.MessageSubscription
}

// NewKeygenConsumer returns a new instance of KeygenConsumer.
func NewKeygenConsumer(
	rabbitConn *amqp.Connection,
	jsBroker messaging.MessageBroker,
	pubsub messaging.PubSub,
	peerRegistry mpc.PeerRegistry,
	keygenResultQueue messaging.MessageQueue,
) KeygenConsumer {
	return &keygenConsumer{
		rabbitConn:        rabbitConn,
		pubsub:            pubsub,
		jsBroker:          jsBroker,
		peerRegistry:      peerRegistry,
		keygenResultQueue: keygenResultQueue,
	}
}

func (sc *keygenConsumer) waitForAllPeersReadyToGenKey(ctx context.Context) error {

	logger.Info("KeygenConsumer: Waiting for all peers to be ready before consuming messages")

	ticker := time.NewTicker(readinessCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			allPeersReady := sc.peerRegistry.ArePeersReady()

			if allPeersReady {
				logger.Info("KeygenConsumer: All peers are ready, proceeding to consume messages")
				return nil
			} else {
				logger.Info("KeygenConsumer: Waiting for all peers to be ready",
					"readyPeers", sc.peerRegistry.GetReadyPeersCount(),
					"totalPeers", sc.peerRegistry.GetTotalPeersCount())
			}
		}
	}
}

// Run subscribes to signing events and processes them until the context is canceled.
func (sc *keygenConsumer) Run(ctx context.Context) error {
	// Wait for sufficient peers before starting to consume messages
	if err := sc.waitForAllPeersReadyToGenKey(ctx); err != nil {
		if err == context.Canceled {
			return nil
		}
		return fmt.Errorf("failed to wait for sufficient peers: %w", err)
	}

	sub, err := sc.jsBroker.CreateSubscription(
		ctx,
		event.KeygenConsumerStream,
		event.KeygenRequestTopic,
		sc.handleKeygenEvent,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to keygen events: %w", err)
	}
	sc.jsSub = sub
	logger.Info("SigningConsumer: Subscribed to keygen events")

	// Block until context cancellation.
	<-ctx.Done()
	logger.Info("KeygenConsumer: Context cancelled, shutting down")

	// When context is canceled, close subscription.
	return sc.Close()
}

func (sc *keygenConsumer) handleKeygenEvent(msg messaging.Message) {
	raw := msg.Data()
	var keygenMsg types.GenerateKeyMessage
	sessionID := msg.Headers()["SessionID"]
	var sessionIDStr string
	if len(sessionID) > 0 {
		sessionIDStr = sessionID[0]
	}

	err := json.Unmarshal(raw, &keygenMsg)
	if err != nil {
		logger.Error("KeygenConsumer: Failed to unmarshal keygen message", err)
		sc.handleKeygenError(keygenMsg, event.ErrorCodeUnmarshalFailure, err, sessionIDStr)
		_ = msg.Ack()
		return
	}

	if !sc.peerRegistry.ArePeersReady() {
		logger.Warn("KeygenConsumer: Not all peers are ready to gen key, skipping message processing")
		sc.handleKeygenError(keygenMsg, event.ErrorCodeClusterNotReady, errors.New("not all peers are ready"), sessionIDStr)
		_ = msg.Ack()
		return
	}

	// Create a reply queue for this request
	ch, err := sc.rabbitConn.Channel()
	if err != nil {
		logger.Error("KeygenConsumer: Failed to create channel", err)
		_ = msg.Nak()
		return
	}
	defer ch.Close()

	replyQueue, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		logger.Error("KeygenConsumer: Failed to declare reply queue", err)
		_ = msg.Nak()
		return
	}

	msgs, err := ch.Consume(
		replyQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Error("KeygenConsumer: Failed to consume from reply queue", err)
		_ = msg.Nak()
		return
	}

	headers := map[string]string{
		"SessionID": uuid.New().String(),
	}
	if err := sc.pubsub.PublishWithReply(MPCGenerateEvent, replyQueue.Name, raw, headers); err != nil {
		logger.Error("KeygenConsumer: Failed to publish keygen event with reply", err)
		_ = msg.Nak()
		return
	}

	deadline := time.Now().Add(keygenResponseTimeout)
	for time.Now().Before(deadline) {
		select {
		case replyMsg, ok := <-msgs:
			if !ok {
				logger.Info("KeygenConsumer: Reply channel closed")
				_ = msg.Nak()
				return
			}
			if replyMsg.Body != nil {
				logger.Info("KeygenConsumer: Completed keygen event; reply received")
				if ackErr := msg.Ack(); ackErr != nil {
					logger.Error("KeygenConsumer: ACK failed", ackErr)
				}
				return
			}
		case <-time.After(keygenPollingInterval):
			continue
		}
	}

	logger.Warn("KeygenConsumer: Timeout waiting for keygen event response")
	_ = msg.Nak()
}

func (sc *keygenConsumer) handleKeygenError(keygenMsg types.GenerateKeyMessage, errorCode event.ErrorCode, err error, sessionID string) {
	keygenResult := event.KeygenResultEvent{
		ResultType:  event.ResultTypeError,
		ErrorCode:   string(errorCode),
		WalletID:    keygenMsg.WalletID,
		ErrorReason: err.Error(),
	}

	keygenResultBytes, err := json.Marshal(keygenResult)
	if err != nil {
		logger.Error("Failed to marshal keygen result event", err,
			"walletID", keygenResult.WalletID,
		)
		return
	}

	topic := fmt.Sprintf(mpc.TypeGenerateWalletResultFmt, keygenResult.WalletID)
	err = sc.keygenResultQueue.Enqueue(topic, keygenResultBytes, &messaging.EnqueueOptions{
		IdempotententKey: buildIdempotentKey(keygenMsg.WalletID, sessionID, mpc.TypeGenerateWalletResultFmt),
	})
	if err != nil {
		logger.Error("Failed to enqueue keygen result event", err,
			"walletID", keygenMsg.WalletID,
		)
	}
}

// Close unsubscribes from the JetStream subject and cleans up resources.
func (sc *keygenConsumer) Close() error {
	if sc.jsSub != nil {
		if err := sc.jsSub.Unsubscribe(); err != nil {
			logger.Error("KeygenConsumer: Failed to unsubscribe from JetStream", err)
			return err
		}
		logger.Info("KeygenConsumer: Unsubscribed from JetStream")
	}
	return nil
}
