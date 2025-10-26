package eventconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/akshay-glide/mpcium/pkg/event"
	"github.com/akshay-glide/mpcium/pkg/logger"
	"github.com/akshay-glide/mpcium/pkg/messaging"
	"github.com/akshay-glide/mpcium/pkg/mpc"
	"github.com/akshay-glide/mpcium/pkg/types"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

const (
	// Maximum time to wait for a signing response.
	signingResponseTimeout = 30 * time.Second
	// How often to poll for the reply message.
	signingPollingInterval = 500 * time.Millisecond
	// How often to check if enough peers are ready
	readinessCheckInterval = 2 * time.Second
)

// SigningConsumer represents a consumer that processes signing events.
type SigningConsumer interface {
	// Run starts the consumer and blocks until the provided context is canceled.
	Run(ctx context.Context) error
	// Close performs a graceful shutdown of the consumer.
	Close() error
}

// signingConsumer implements SigningConsumer.
type signingConsumer struct {
	rabbitConn         *amqp.Connection
	pubsub             messaging.PubSub
	jsBroker           messaging.MessageBroker
	peerRegistry       mpc.PeerRegistry
	mpcThreshold       int
	signingResultQueue messaging.MessageQueue
	jsSub              messaging.MessageSubscription
}

// NewSigningConsumer returns a new instance of SigningConsumer.
func NewSigningConsumer(rabbitConn *amqp.Connection, jsBroker messaging.MessageBroker, pubsub messaging.PubSub, peerRegistry mpc.PeerRegistry, signingResultQueue messaging.MessageQueue) SigningConsumer {
	mpcThreshold := viper.GetInt("mpc_threshold")
	return &signingConsumer{
		rabbitConn:         rabbitConn,
		pubsub:             pubsub,
		jsBroker:           jsBroker,
		peerRegistry:       peerRegistry,
		mpcThreshold:       mpcThreshold,
		signingResultQueue: signingResultQueue,
	}
}

// waitForSufficientPeers waits until enough peers are ready to handle signing requests
func (sc *signingConsumer) waitForSufficientPeers(ctx context.Context) error {
	requiredPeers := int64(sc.mpcThreshold + 1) // t+1 peers needed for signing

	logger.Info("SigningConsumer: Waiting for sufficient peers before consuming messages",
		"required", requiredPeers,
		"threshold", sc.mpcThreshold)

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
			readyPeers := sc.peerRegistry.GetReadyPeersCount()
			if readyPeers >= requiredPeers {
				logger.Info("SigningConsumer: Sufficient peers ready, starting message consumption",
					"ready", readyPeers,
					"t+1", requiredPeers)
				return nil
			}
			logger.Info("SigningConsumer: Waiting for more peers to be ready",
				"ready", readyPeers,
				"t+1", requiredPeers)
		}
	}
}

// Run subscribes to signing events and processes them until the context is canceled.
func (sc *signingConsumer) Run(ctx context.Context) error {
	// Wait for sufficient peers before starting to consume messages
	if err := sc.waitForSufficientPeers(ctx); err != nil {
		if err == context.Canceled {
			return nil
		}
		return fmt.Errorf("failed to wait for sufficient peers: %w", err)
	}

	sub, err := sc.jsBroker.CreateSubscription(
		ctx,
		event.SigningConsumerStream,
		event.SigningRequestTopic,
		sc.handleSigningEvent,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to signing events: %w", err)
	}
	sc.jsSub = sub
	logger.Info("SigningConsumer: Subscribed to signing events")

	// Block until context cancellation.
	<-ctx.Done()
	logger.Info("SigningConsumer: Context cancelled, shutting down")

	// When context is canceled, close subscription.
	return sc.Close()
}

// The handleSigningEvent function in sign_consumer.go acts as a bridge between the JetStream-based event queue and the MPC (Multi-Party Computation) signing system
// Creates a reply channel: It generates a unique inbox address using nats.NewInbox() to receive the signing response.
// Sets up response handling: It creates a synchronous subscription to listen for replies on this inbox.
// Forwards the signing request: It publishes the original signing event data to the MPCSigningEventTopic with the reply inbox attached, which triggers the MPC signing process.
// Polls for completion: It enters a polling loop that checks for a reply message, continuing until either:
// A reply is received (successful signing)
// An error occurs (failed signing)
// The timeout is reached (30 seconds)
// Completes the transaction: It either acknowledges (Ack) the message if signing was successful or negatively acknowledges (Nak) it if there was a timeout or error.
// MPC Session Interaction
// The signing consumer doesn't directly interact with MPC sessions. Instead:
// It publishes the signing request to the MPCSigningEventTopic, which is consumed by the eventconsumer.consumeTxSigningEvent handler.
// This handler creates the appropriate signing session (SigningSession for ECDSA or EDDSASigningSession for EdDSA) via the MPC node's creation methods.
// The MPC signing sessions manage the distributed cryptographic operations across multiple nodes, handling message routing, party updates, and signature verification.
// When signing completes, the session publishes the result to a queue and calls the onSuccess callback, which sends a reply to the inbox that the SigningConsumer is monitoring.
// The reply signals completion, allowing the SigningConsumer to acknowledge the original message.
func (sc *signingConsumer) handleSigningEvent(msg messaging.Message) {
	raw := msg.Data()
	var signingMsg types.SignTxMessage
	sessionID := msg.Headers()["SessionID"]
	var sessionIDStr string
	if len(sessionID) > 0 {
		sessionIDStr = sessionID[0]
	}

	err := json.Unmarshal(raw, &signingMsg)
	if err != nil {
		logger.Error("SigningConsumer: Failed to unmarshal signing message", err)
		sc.handleSigningError(signingMsg, event.ErrorCodeUnmarshalFailure, err, sessionIDStr)
		_ = msg.Ack()
		return
	}

	if !sc.peerRegistry.AreMajorityReady() {
		requiredPeers := int64(sc.mpcThreshold + 1)
		err := fmt.Errorf("not enough peers to process signing request: ready=%d, required=%d", sc.peerRegistry.GetReadyPeersCount(), requiredPeers)
		sc.handleSigningError(signingMsg, event.ErrorCodeNotMajority, err, sessionIDStr)
		_ = msg.Ack()
		return
	}

	// Create a reply queue for this request
	ch, err := sc.rabbitConn.Channel()
	if err != nil {
		logger.Error("SigningConsumer: Failed to create channel", err)
		_ = msg.Nak()
		return
	}
	defer ch.Close()

	replyQueue, err := ch.QueueDeclare(
		"",    // auto-generated name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		logger.Error("SigningConsumer: Failed to declare reply queue", err)
		_ = msg.Nak()
		return
	}

	// Consume from reply queue
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
		logger.Error("SigningConsumer: Failed to consume from reply queue", err)
		_ = msg.Nak()
		return
	}

	// Publish the signing event with reply queue
	headers := map[string]string{
		"SessionID": uuid.New().String(),
	}
	if err := sc.pubsub.PublishWithReply(MPCSignEvent, replyQueue.Name, raw, headers); err != nil {
		logger.Error("SigningConsumer: Failed to publish signing event with reply", err)
		_ = msg.Nak()
		return
	}

	// Wait for reply with timeout
	deadline := time.Now().Add(signingResponseTimeout)
	for time.Now().Before(deadline) {
		select {
		case replyMsg, ok := <-msgs:
			if !ok {
				logger.Info("SigningConsumer: Reply channel closed")
				_ = msg.Nak()
				return
			}
			if replyMsg.Body != nil {
				logger.Info("SigningConsumer: Completed signing event; reply received")
				if ackErr := msg.Ack(); ackErr != nil {
					logger.Error("SigningConsumer: ACK failed", ackErr)
				}
				return
			}
		case <-time.After(signingPollingInterval):
			continue
		}
	}

	logger.Warn("SigningConsumer: Timeout waiting for signing event response")
	_ = msg.Nak()
}

func (sc *signingConsumer) handleSigningError(signMsg types.SignTxMessage, errorCode event.ErrorCode, err error, sessionID string) {
	signingResult := event.SigningResultEvent{
		ResultType:          event.ResultTypeError,
		ErrorCode:           errorCode,
		NetworkInternalCode: signMsg.NetworkInternalCode,
		WalletID:            signMsg.WalletID,
		TxID:                signMsg.TxID,
		ErrorReason:         err.Error(),
	}

	signingResultBytes, err := json.Marshal(signingResult)
	if err != nil {
		logger.Error("Failed to marshal signing result event", err,
			"walletID", signMsg.WalletID,
			"txID", signMsg.TxID,
		)
		return
	}

	err = sc.signingResultQueue.Enqueue(event.SigningResultCompleteTopic, signingResultBytes, &messaging.EnqueueOptions{
		IdempotententKey: buildIdempotentKey(signMsg.TxID, sessionID, mpc.TypeSigningResultFmt),
	})
	if err != nil {
		logger.Error("Failed to enqueue signing result event", err,
			"walletID", signMsg.WalletID,
			"txID", signMsg.TxID,
		)
	}
}

// Close unsubscribes from the JetStream subject and cleans up resources.
func (sc *signingConsumer) Close() error {
	if sc.jsSub != nil {
		if err := sc.jsSub.Unsubscribe(); err != nil {
			logger.Error("SigningConsumer: Failed to unsubscribe from JetStream", err)
			return err
		}
		logger.Info("SigningConsumer: Unsubscribed from JetStream")
	}
	return nil
}

func buildIdempotentKey(baseID string, sessionID string, formatTemplate string) string {
	var uniqueKey string
	if sessionID != "" {
		uniqueKey = fmt.Sprintf("%s:%s", baseID, sessionID)
	} else {
		uniqueKey = baseID
	}
	return fmt.Sprintf(formatTemplate, uniqueKey)
}
