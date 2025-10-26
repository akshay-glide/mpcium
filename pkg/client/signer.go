package client

import "github.com/akshay-glide/mpcium/pkg/types"

// Signer defines the interface for signing messages with different key types
type Signer interface {
	// Sign signs the given data and returns the signature
	Sign(data []byte) ([]byte, error)
	// Algorithm returns the key algorithm used by this signer
	Algorithm() types.EventInitiatorKeyType
	// PublicKey returns the public key in hex format
	PublicKey() (string, error)
}
