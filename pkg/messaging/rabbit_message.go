package messaging

import "time"

// Message represents a generic message interface for both NATS and RabbitMQ
type Message interface {
	Data() []byte
	Headers() map[string][]string
	Subject() string
	Ack() error
	Nak() error
	Term() error
	InProgress() error
	Metadata() (*MessageMetadata, error)
}

// MessageMetadata contains message delivery metadata
type MessageMetadata struct {
	Sequence     SequencePair
	NumDelivered int
	Timestamp    time.Time
}

// SequencePair represents sequence numbers
type SequencePair struct {
	Consumer uint64
	Stream   uint64
}

// StreamInfo contains stream metadata
type StreamInfo struct {
	Config StreamConfig
	State  StreamState
}

// StreamConfig represents stream configuration
type StreamConfig struct {
	Name        string
	Subjects    []string
	Description string
}

// StreamState represents stream state
type StreamState struct {
	Messages  uint64
	Bytes     uint64
	FirstSeq  uint64
	LastSeq   uint64
	Consumers int
}
