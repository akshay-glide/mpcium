package client

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
	"github.com/akshay-glide/mpcium/pkg/encryption"
	"github.com/akshay-glide/mpcium/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalSigner_Ed25519(t *testing.T) {
	// Generate a test Ed25519 key
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	seed := privateKey.Seed()
	privKeyHex := hex.EncodeToString(seed)

	// Create temporary key file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_ed25519.key")

	err = os.WriteFile(keyPath, []byte(privKeyHex), 0600)
	require.NoError(t, err)

	// Test creating signer
	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
		KeyPath: keyPath,
	})
	require.NoError(t, err)
	require.NotNil(t, signer)

	localSigner, ok := signer.(*LocalSigner)
	require.True(t, ok)
	assert.Equal(t, types.EventInitiatorKeyTypeEd25519, localSigner.keyType)
	assert.NotNil(t, localSigner.ed25519Key)
	assert.Nil(t, localSigner.p256Key)
}

func TestNewLocalSigner_P256(t *testing.T) {
	// Generate a test P256 key
	keyData, err := encryption.GenerateP256Keys()
	require.NoError(t, err)

	// Create temporary key file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_p256.key")

	err = os.WriteFile(keyPath, []byte(keyData.PrivateKeyHex), 0600)
	require.NoError(t, err)

	// Test creating signer
	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeP256, LocalSignerOptions{
		KeyPath: keyPath,
	})
	require.NoError(t, err)
	require.NotNil(t, signer)

	localSigner, ok := signer.(*LocalSigner)
	require.True(t, ok)
	assert.Equal(t, types.EventInitiatorKeyTypeP256, localSigner.keyType)
	assert.Nil(t, localSigner.ed25519Key)
	assert.NotNil(t, localSigner.p256Key)
}

func TestNewLocalSigner_EncryptedKey(t *testing.T) {
	// Generate a test Ed25519 key
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	seed := privateKey.Seed()
	privKeyHex := hex.EncodeToString(seed)

	// Create encrypted key file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_encrypted.key.age")
	password := "test-password"

	// Encrypt the key using age
	recipient, err := age.NewScryptRecipient(password)
	require.NoError(t, err)

	tmpFile, err := os.Create(keyPath)
	require.NoError(t, err)
	defer tmpFile.Close()

	writer, err := age.Encrypt(tmpFile, recipient)
	require.NoError(t, err)

	_, err = writer.Write([]byte(privKeyHex))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Test creating signer with encrypted key
	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
		KeyPath:   keyPath,
		Encrypted: true,
		Password:  password,
	})
	require.NoError(t, err)
	require.NotNil(t, signer)

	localSigner, ok := signer.(*LocalSigner)
	require.True(t, ok)
	assert.Equal(t, types.EventInitiatorKeyTypeEd25519, localSigner.keyType)
	assert.NotNil(t, localSigner.ed25519Key)
}

func TestNewLocalSigner_Errors(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("nonexistent key file", func(t *testing.T) {
		signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
			KeyPath: filepath.Join(tmpDir, "nonexistent.key"),
		})
		assert.Error(t, err)
		assert.Nil(t, signer)
		assert.Contains(t, err.Error(), "key file not found")
	})

	t.Run("encrypted key without password", func(t *testing.T) {
		keyPath := filepath.Join(tmpDir, "test.key.age")
		err := os.WriteFile(keyPath, []byte("dummy"), 0600)
		require.NoError(t, err)

		signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
			KeyPath:   keyPath,
			Encrypted: true,
		})
		assert.Error(t, err)
		assert.Nil(t, signer)
		assert.Contains(t, err.Error(), "no password provided")
	})

	t.Run("unsupported key type", func(t *testing.T) {
		keyPath := filepath.Join(tmpDir, "test.key")
		err := os.WriteFile(keyPath, []byte("dummy"), 0600)
		require.NoError(t, err)

		signer, err := NewLocalSigner("unsupported", LocalSignerOptions{
			KeyPath: keyPath,
		})
		assert.Error(t, err)
		assert.Nil(t, signer)
		assert.Contains(t, err.Error(), "unsupported key type")
	})
}

func TestLocalSigner_Sign_Ed25519(t *testing.T) {
	// Generate a test Ed25519 key
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	seed := privateKey.Seed()
	privKeyHex := hex.EncodeToString(seed)

	// Create temporary key file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_ed25519.key")

	err = os.WriteFile(keyPath, []byte(privKeyHex), 0600)
	require.NoError(t, err)

	// Create signer
	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
		KeyPath: keyPath,
	})
	require.NoError(t, err)

	// Test signing
	data := []byte("test message to sign")
	signature, err := signer.Sign(data)
	require.NoError(t, err)
	assert.NotEmpty(t, signature)
	assert.Equal(t, ed25519.SignatureSize, len(signature))

	// Verify signature
	publicKey := privateKey.Public().(ed25519.PublicKey)
	valid := ed25519.Verify(publicKey, data, signature)
	assert.True(t, valid)
}

func TestLocalSigner_Sign_P256(t *testing.T) {
	// Generate a test P256 key
	keyData, err := encryption.GenerateP256Keys()
	require.NoError(t, err)

	// Create temporary key file
	tmpDir := t.TempDir()
	keyPath := filepath.Join(tmpDir, "test_p256.key")

	err = os.WriteFile(keyPath, []byte(keyData.PrivateKeyHex), 0600)
	require.NoError(t, err)

	// Create signer
	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeP256, LocalSignerOptions{
		KeyPath: keyPath,
	})
	require.NoError(t, err)

	// Test signing
	data := []byte("test message to sign")
	signature, err := signer.Sign(data)
	require.NoError(t, err)
	assert.NotEmpty(t, signature)

	// Verify signature using the encryption package
	localSigner := signer.(*LocalSigner)
	err = encryption.VerifyP256Signature(&localSigner.p256Key.PublicKey, data, signature)
	assert.NoError(t, err)
}

func TestLocalSigner_Algorithm(t *testing.T) {
	tmpDir := t.TempDir()

	// Test Ed25519
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	seed := privateKey.Seed()
	privKeyHex := hex.EncodeToString(seed)
	keyPath := filepath.Join(tmpDir, "test_ed25519.key")
	err = os.WriteFile(keyPath, []byte(privKeyHex), 0600)
	require.NoError(t, err)

	signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
		KeyPath: keyPath,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EventInitiatorKeyTypeEd25519, signer.Algorithm())

	// Test P256
	keyData, err := encryption.GenerateP256Keys()
	require.NoError(t, err)
	keyPathP256 := filepath.Join(tmpDir, "test_p256.key")
	err = os.WriteFile(keyPathP256, []byte(keyData.PrivateKeyHex), 0600)
	require.NoError(t, err)

	signerP256, err := NewLocalSigner(types.EventInitiatorKeyTypeP256, LocalSignerOptions{
		KeyPath: keyPathP256,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EventInitiatorKeyTypeP256, signerP256.Algorithm())
}

func TestLocalSigner_PublicKey(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("Ed25519", func(t *testing.T) {
		// Generate key
		_, privateKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		seed := privateKey.Seed()
		privKeyHex := hex.EncodeToString(seed)
		keyPath := filepath.Join(tmpDir, "test_ed25519.key")
		err = os.WriteFile(keyPath, []byte(privKeyHex), 0600)
		require.NoError(t, err)

		// Create signer
		signer, err := NewLocalSigner(types.EventInitiatorKeyTypeEd25519, LocalSignerOptions{
			KeyPath: keyPath,
		})
		require.NoError(t, err)

		// Get public key
		pubKeyHex, err := signer.PublicKey()
		require.NoError(t, err)
		assert.NotEmpty(t, pubKeyHex)

		// Verify it matches the expected public key
		expectedPubKey := privateKey.Public().(ed25519.PublicKey)
		expectedHex := hex.EncodeToString(expectedPubKey)
		assert.Equal(t, expectedHex, pubKeyHex)
	})

	t.Run("P256", func(t *testing.T) {
		// Generate key
		keyData, err := encryption.GenerateP256Keys()
		require.NoError(t, err)
		keyPath := filepath.Join(tmpDir, "test_p256.key")
		err = os.WriteFile(keyPath, []byte(keyData.PrivateKeyHex), 0600)
		require.NoError(t, err)

		// Create signer
		signer, err := NewLocalSigner(types.EventInitiatorKeyTypeP256, LocalSignerOptions{
			KeyPath: keyPath,
		})
		require.NoError(t, err)

		// Get public key
		pubKeyHex, err := signer.PublicKey()
		require.NoError(t, err)
		assert.NotEmpty(t, pubKeyHex)

		// Verify it's valid hex
		_, err = hex.DecodeString(pubKeyHex)
		assert.NoError(t, err)
	})
}
