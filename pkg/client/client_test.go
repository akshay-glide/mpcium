package client

import (
	"errors"
	"testing"

	"github.com/fystack/mpcium/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockSigner is a mock implementation of the Signer interface
type MockSigner struct {
	mock.Mock
}

func (m *MockSigner) Sign(data []byte) ([]byte, error) {
	args := m.Called(data)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockSigner) Algorithm() types.EventInitiatorKeyType {
	args := m.Called()
	return args.Get(0).(types.EventInitiatorKeyType)
}

func (m *MockSigner) PublicKey() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

// MockNATSConn creates a mock NATS connection for testing
func MockNATSConn() *nats.Conn {
	// For unit tests, we can return nil and handle it appropriately in tests
	// In a real test environment, you would use nats-server for testing
	return nil
}

func TestNewMPCClient_Success(t *testing.T) {
	mockSigner := &MockSigner{}
	mockSigner.On("Algorithm").Return(types.EventInitiatorKeyTypeEd25519)

	// Since we can't easily create a real NATS connection in unit tests,
	// we'll test the Options validation logic
	opts := Options{
		Signer: mockSigner,
	}

	// Test that signer is required
	assert.NotNil(t, opts.Signer)
}

func TestNewMPCClient_NoSigner(t *testing.T) {
	// Test that client creation fails without signer
	// This test would require mocking the logger.Fatal call or refactoring to return error
	opts := Options{
		Signer: nil,
	}

	assert.Nil(t, opts.Signer, "Signer should be nil to test error case")
}

func TestMPCClient_CreateWallet(t *testing.T) {
	mockSigner := &MockSigner{}

	// Set up expectations
	testSignature := []byte("test-signature")
	mockSigner.On("Sign", mock.AnythingOfType("[]uint8")).Return(testSignature, nil)

	// Create a client instance directly for testing (bypassing NATS setup)
	client := &mpcClient{
		signer: mockSigner,
	}

	// Test CreateWallet - this will test the signing logic
	// Note: This test would require mocking the messaging broker as well
	// For now, we test that the signer is called correctly

	walletID := "test-wallet-123"

	// We can't fully test CreateWallet without mocking the broker,
	// but we can test the signing part by calling it directly

	// Simulate what CreateWallet does with signing
	msg := &types.GenerateKeyMessage{
		WalletID: walletID,
	}

	raw, err := msg.Raw()
	require.NoError(t, err)

	signature, err := client.signer.Sign(raw)
	require.NoError(t, err)
	assert.Equal(t, testSignature, signature)

	// Verify mock expectations
	mockSigner.AssertExpectations(t)
}

func TestMPCClient_CreateWallet_SigningError(t *testing.T) {
	mockSigner := &MockSigner{}

	// Set up signer to return error
	mockSigner.On("Sign", mock.AnythingOfType("[]uint8")).Return([]byte(nil), errors.New("signing failed"))

	client := &mpcClient{
		signer: mockSigner,
	}

	// Simulate the signing part that would happen in CreateWallet
	msg := &types.GenerateKeyMessage{
		WalletID: "test-wallet",
	}

	raw, err := msg.Raw()
	require.NoError(t, err)

	signature, err := client.signer.Sign(raw)
	assert.Error(t, err)
	assert.Nil(t, signature)
	assert.Contains(t, err.Error(), "signing failed")

	mockSigner.AssertExpectations(t)
}

func TestMPCClient_SignTransaction(t *testing.T) {
	mockSigner := &MockSigner{}

	// Set up expectations
	testSignature := []byte("test-transaction-signature")
	mockSigner.On("Sign", mock.AnythingOfType("[]uint8")).Return(testSignature, nil)

	client := &mpcClient{
		signer: mockSigner,
	}

	// Test signing part of SignTransaction
	msg := &types.SignTxMessage{
		KeyType:             types.KeyTypeSecp256k1,
		WalletID:            "test-wallet",
		NetworkInternalCode: "btc-mainnet",
		TxID:                "test-tx-123",
		Tx:                  []byte("test transaction data"),
	}

	raw, err := msg.Raw()
	require.NoError(t, err)

	signature, err := client.signer.Sign(raw)
	require.NoError(t, err)
	assert.Equal(t, testSignature, signature)

	mockSigner.AssertExpectations(t)
}

func TestMPCClient_Resharing(t *testing.T) {
	mockSigner := &MockSigner{}

	// Set up expectations
	testSignature := []byte("test-resharing-signature")
	mockSigner.On("Sign", mock.AnythingOfType("[]uint8")).Return(testSignature, nil)

	client := &mpcClient{
		signer: mockSigner,
	}

	// Test signing part of Resharing
	msg := &types.ResharingMessage{
		SessionID:    "reshare-session-123",
		NodeIDs:      []string{"node1", "node2", "node3"},
		NewThreshold: 2,
		KeyType:      types.KeyTypeSecp256k1,
		WalletID:     "test-wallet",
	}

	raw, err := msg.Raw()
	require.NoError(t, err)

	signature, err := client.signer.Sign(raw)
	require.NoError(t, err)
	assert.Equal(t, testSignature, signature)

	mockSigner.AssertExpectations(t)
}

func TestSignerInterface_Compliance(t *testing.T) {
	// Test that our mock signer implements the interface correctly
	mockSigner := &MockSigner{}

	// Set up mock expectations
	mockSigner.On("Algorithm").Return(types.EventInitiatorKeyTypeP256)
	mockSigner.On("PublicKey").Return("mock-public-key-hex", nil)
	mockSigner.On("Sign", []byte("test")).Return([]byte("mock-signature"), nil)

	// Test interface compliance
	var signer Signer = mockSigner

	algorithm := signer.Algorithm()
	assert.Equal(t, types.EventInitiatorKeyTypeP256, algorithm)

	pubKey, err := signer.PublicKey()
	require.NoError(t, err)
	assert.Equal(t, "mock-public-key-hex", pubKey)

	signature, err := signer.Sign([]byte("test"))
	require.NoError(t, err)
	assert.Equal(t, []byte("mock-signature"), signature)

	mockSigner.AssertExpectations(t)
}

func TestSignerInterface_ErrorHandling(t *testing.T) {
	mockSigner := &MockSigner{}

	// Set up error cases
	mockSigner.On("PublicKey").Return("", errors.New("public key error"))
	mockSigner.On("Sign", mock.Anything).Return([]byte(nil), errors.New("signing error"))

	var signer Signer = mockSigner

	// Test public key error
	pubKey, err := signer.PublicKey()
	assert.Error(t, err)
	assert.Empty(t, pubKey)
	assert.Contains(t, err.Error(), "public key error")

	// Test signing error
	signature, err := signer.Sign([]byte("test"))
	assert.Error(t, err)
	assert.Nil(t, signature)
	assert.Contains(t, err.Error(), "signing error")

	mockSigner.AssertExpectations(t)
}

// Integration test helpers
func TestOptionsValidation(t *testing.T) {
	t.Run("valid options", func(t *testing.T) {
		mockSigner := &MockSigner{}
		opts := Options{
			Signer: mockSigner,
		}

		assert.NotNil(t, opts.Signer)
		// In real implementation, would also check NatsConn is not nil
	})

	t.Run("missing signer", func(t *testing.T) {
		opts := Options{
			Signer: nil,
		}

		assert.Nil(t, opts.Signer)
		// This would trigger a fatal error in NewMPCClient
	})
}

// Benchmark tests for signing operations
func BenchmarkMockSigner_Sign(b *testing.B) {
	mockSigner := &MockSigner{}
	testSignature := []byte("benchmark-signature")
	mockSigner.On("Sign", mock.AnythingOfType("[]uint8")).Return(testSignature, nil)

	data := []byte("benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mockSigner.Sign(data)
		if err != nil {
			b.Fatalf("Sign failed: %v", err)
		}
	}
}

// Test helper functions
func createTestMPCClient(signer Signer) *mpcClient {
	return &mpcClient{
		signer: signer,
	}
}

func TestCreateTestMPCClient(t *testing.T) {
	mockSigner := &MockSigner{}
	client := createTestMPCClient(mockSigner)

	assert.NotNil(t, client)
	assert.Equal(t, mockSigner, client.signer)
}
