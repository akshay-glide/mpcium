package client

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	"github.com/akshay-glide/mpcium/pkg/encryption"
	"github.com/akshay-glide/mpcium/pkg/types"
)

// LocalSigner implements the Signer interface for local key management
type LocalSigner struct {
	keyType    types.EventInitiatorKeyType
	ed25519Key ed25519.PrivateKey
	p256Key    *ecdsa.PrivateKey
}

// LocalSignerOptions defines options for creating a LocalSigner
type LocalSignerOptions struct {
	KeyPath   string // Path to the key file
	Encrypted bool   // Whether the key is encrypted
	Password  string // Password for decryption (required if encrypted)
}

// NewLocalSigner creates a new LocalSigner for the specified key type
func NewLocalSigner(keyType types.EventInitiatorKeyType, opts LocalSignerOptions) (Signer, error) {
	signer := &LocalSigner{
		keyType: keyType,
	}

	// Set default path if not provided
	if opts.KeyPath == "" {
		opts.KeyPath = filepath.Join(".", "event_initiator.key")
	}

	// Auto-detect encryption if .age extension
	if strings.HasSuffix(opts.KeyPath, ".age") {
		opts.Encrypted = true
	}

	// Read the key file
	keyData, err := readKeyFile(opts.KeyPath, opts.Encrypted, opts.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	// Parse the key based on type
	switch keyType {
	case types.EventInitiatorKeyTypeEd25519:
		if err := signer.loadEd25519Key(keyData); err != nil {
			return nil, fmt.Errorf("failed to load Ed25519 key: %w", err)
		}
	case types.EventInitiatorKeyTypeP256:
		if err := signer.loadP256Key(keyData); err != nil {
			return nil, fmt.Errorf("failed to load P256 key: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported key type: %s", keyType)
	}

	return signer, nil
}

// readKeyFile reads a key file, handling both encrypted and unencrypted files
func readKeyFile(keyPath string, encrypted bool, password string) ([]byte, error) {
	// Check if key file exists
	if _, err := os.Stat(keyPath); err != nil {
		return nil, fmt.Errorf("key file not found: %s", keyPath)
	}

	if encrypted {
		if password == "" {
			return nil, fmt.Errorf("encrypted key found but no password provided")
		}

		// Read encrypted file
		encryptedBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read encrypted key file: %w", err)
		}

		// Decrypt the key
		return decryptPrivateKey(encryptedBytes, password)
	} else {
		// Read unencrypted key
		return os.ReadFile(keyPath)
	}
}

// loadEd25519Key loads an Ed25519 private key from hex data
func (s *LocalSigner) loadEd25519Key(keyData []byte) error {
	privHex := string(keyData)
	privSeed, err := hex.DecodeString(strings.TrimSpace(privHex))
	if err != nil {
		return fmt.Errorf("failed to decode Ed25519 private key hex: %w", err)
	}

	s.ed25519Key = ed25519.NewKeyFromSeed(privSeed)
	return nil
}

// loadP256Key loads a P256 private key from various formats
func (s *LocalSigner) loadP256Key(keyData []byte) error {
	privKey, err := encryption.ParseP256PrivateKey(keyData)
	if err != nil {
		return fmt.Errorf("failed to parse P256 private key: %w", err)
	}

	s.p256Key = privKey
	return nil
}

// Sign implements the Signer interface for LocalSigner
func (s *LocalSigner) Sign(data []byte) ([]byte, error) {
	switch s.keyType {
	case types.EventInitiatorKeyTypeEd25519:
		if s.ed25519Key == nil {
			return nil, fmt.Errorf("Ed25519 private key not initialized")
		}
		return ed25519.Sign(s.ed25519Key, data), nil

	case types.EventInitiatorKeyTypeP256:
		if s.p256Key == nil {
			return nil, fmt.Errorf("P256 private key not initialized")
		}
		return encryption.SignWithP256(s.p256Key, data)

	default:
		return nil, fmt.Errorf("unsupported key type: %s", s.keyType)
	}
}

// Algorithm implements the Signer interface for LocalSigner
func (s *LocalSigner) Algorithm() types.EventInitiatorKeyType {
	return s.keyType
}

// PublicKey implements the Signer interface for LocalSigner
func (s *LocalSigner) PublicKey() (string, error) {
	switch s.keyType {
	case types.EventInitiatorKeyTypeEd25519:
		if s.ed25519Key == nil {
			return "", fmt.Errorf("Ed25519 private key not initialized")
		}
		pubKey := s.ed25519Key.Public().(ed25519.PublicKey)
		return hex.EncodeToString(pubKey), nil

	case types.EventInitiatorKeyTypeP256:
		if s.p256Key == nil {
			return "", fmt.Errorf("P256 private key not initialized")
		}
		pubKeyBytes, err := encryption.MarshalP256PublicKey(&s.p256Key.PublicKey)
		if err != nil {
			return "", fmt.Errorf("failed to marshal P256 public key: %w", err)
		}
		return hex.EncodeToString(pubKeyBytes), nil

	default:
		return "", fmt.Errorf("unsupported key type: %s", s.keyType)
	}
}

// decryptPrivateKey decrypts an encrypted private key using age
func decryptPrivateKey(encryptedData []byte, password string) ([]byte, error) {
	identity, err := age.NewScryptIdentity(password)
	if err != nil {
		return nil, fmt.Errorf("failed to create identity from password: %w", err)
	}

	decrypter, err := age.Decrypt(strings.NewReader(string(encryptedData)), identity)
	if err != nil {
		return nil, fmt.Errorf("failed to create decrypter: %w", err)
	}

	decryptedData, err := io.ReadAll(decrypter)
	if err != nil {
		return nil, fmt.Errorf("failed to read decrypted data: %w", err)
	}

	return decryptedData, nil
}
