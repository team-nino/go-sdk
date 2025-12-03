package rabbit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/team-nino/go-sdk/infra/broker"
)

const (
	testInvalidChannelError = "channel is not a RabbitMQ channel"
	testQueueName           = "test-queue"
	testExchangeName        = "test-exchange"
	testRoutingKey          = "test.key"
)

// MockChannelPool is a test mock of broker.ChannelPool
type MockChannelPool struct {
	acquireCallCount int
	releaseCallCount int
}

func (m *MockChannelPool) AcquireChannel(ctx context.Context) (broker.Channel, error) {
	m.acquireCallCount++
	return nil, nil
}

func (m *MockChannelPool) ReleaseChannel(ch broker.Channel) error {
	m.releaseCallCount++
	return nil
}

func (m *MockChannelPool) Close() error {
	return nil
}

func (m *MockChannelPool) GetStats() broker.PoolStats {
	return broker.PoolStats{}
}

func (m *MockChannelPool) HealthCheck(ctx context.Context) error {
	return nil
}

func TestNewTopologyManager(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	require.NotNil(t, tm)
	assert.Equal(t, mockPool, tm.pool)
}

func TestTopologyManagerImplementsInterface(t *testing.T) {
	var _ broker.TopologyManager = (*TopologyManager)(nil)
}

func TestDeclareQueueFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	config := broker.QueueConfig{
		Name:    testQueueName,
		Durable: true,
	}

	_, err := tm.DeclareQueue(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestDeleteQueueFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	err := tm.DeleteQueue(context.Background(), testQueueName, false, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestDeclareExchangeFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	config := broker.ExchangeConfig{
		Name: testExchangeName,
		Kind: "direct",
	}

	err := tm.DeclareExchange(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestDeleteExchangeFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	err := tm.DeleteExchange(context.Background(), testExchangeName, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestBindQueueFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	config := broker.BindingConfig{
		QueueName:    testQueueName,
		ExchangeName: testExchangeName,
		RoutingKey:   testRoutingKey,
	}

	err := tm.BindQueue(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestUnbindQueueFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	err := tm.UnbindQueue(context.Background(), testQueueName, testExchangeName, testRoutingKey)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestPurgeQueueFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	err := tm.PurgeQueue(context.Background(), testQueueName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestGetQueueInfoFailsWhenChannelAcquisitionFails(t *testing.T) {
	mockPool := &MockChannelPool{}
	tm := NewTopologyManager(mockPool)

	_, err := tm.GetQueueInfo(context.Background(), testQueueName)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), testInvalidChannelError)
}

func TestConvertToAMQPTableWithNilArgs(t *testing.T) {
	table := convertToAMQPTable(nil)
	assert.NotNil(t, table)
	assert.Equal(t, 0, len(table))
}

func TestConvertToAMQPTableWithArgs(t *testing.T) {
	args := map[string]any{
		"key1": "value1",
		"key2": 42,
	}
	table := convertToAMQPTable(args)
	assert.Equal(t, 2, len(table))
	assert.Equal(t, "value1", table["key1"])
	assert.Equal(t, 42, table["key2"])
}
