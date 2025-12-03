package rabbit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MockAMQPChannel struct {
	closeChan chan *amqp.Error
	closed    atomic.Bool
	mu        sync.Mutex
	callCount map[string]int
}

func NewMockAMQPChannel() *MockAMQPChannel {
	return &MockAMQPChannel{
		closeChan: make(chan *amqp.Error, 1),
		callCount: make(map[string]int),
	}
}

func (m *MockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	m.mu.Lock()
	m.callCount["NotifyClose"]++
	m.mu.Unlock()
	return m.closeChan
}

func (m *MockAMQPChannel) Close() error {
	m.mu.Lock()
	m.callCount["Close"]++
	m.mu.Unlock()

	if m.closed.Load() {
		return nil
	}
	m.closed.Store(true)
	close(m.closeChan)
	return nil
}

func (m *MockAMQPChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	m.mu.Lock()
	m.callCount["ExchangeDeclarePassive"]++
	m.mu.Unlock()
	return nil
}

func (m *MockAMQPChannel) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[method]
}

func (m *MockAMQPChannel) SimulateClose() {
	if !m.closed.Load() {
		m.closed.Store(true)
		select {
		case m.closeChan <- nil:
		default:
		}
	}
}

func TestNewChannelManagerNilConnection(t *testing.T) {
	_, err := NewChannelManager(nil, ChannelConfig{})
	assert.Error(t, err)
	assert.Equal(t, "connection cannot be nil", err.Error())
}

func TestNewChannelManagerDefaultConfig(t *testing.T) {
	config := ChannelConfig{}
	cm, err := NewChannelManager((*amqp.Connection)(nil), config)
	assert.Error(t, err)
	assert.Nil(t, cm)
}

func TestChannelManagerAcquireChannelManagerClosed(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     true,
	}

	_, err := cm.AcquireChannel(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "channel manager is closed", err.Error())
}

func TestChannelManagerAcquireChannelTimeout(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config: ChannelConfig{
			MaxChannels:    1,
			ChannelTimeout: 100 * time.Millisecond,
		},
		channels: make(chan *ManagedChannel, 1),
		closed:   false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := cm.AcquireChannel(ctx)
	assert.Error(t, err)
}

func TestChannelManagerAcquireChannelContextCanceled(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config: ChannelConfig{
			MaxChannels:    1,
			ChannelTimeout: 5 * time.Second,
		},
		channels: make(chan *ManagedChannel, 1),
		closed:   false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cm.AcquireChannel(ctx)
	assert.Error(t, err)
}

func TestChannelManagerReleaseChannelNilChannel(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     false,
	}

	err := cm.ReleaseChannel(nil)
	assert.Error(t, err)
	assert.Equal(t, "channel cannot be nil", err.Error())
}

func TestChannelManagerReleaseChannelClosedManager(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     true,
	}

	managedCh := &ManagedChannel{
		createdAt:   time.Now(),
		lastUsedAt:  time.Now(),
		isBroken:    false,
		manager:     cm,
		notifyClose: make(chan *amqp.Error, 1),
	}

	err := cm.ReleaseChannel(managedCh)
	assert.NoError(t, err)
}

func TestChannelManagerReleaseChannelBrokenChannel(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     false,
	}

	managedCh := &ManagedChannel{
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		isBroken:   true,
		manager:    cm,
	}

	err := cm.ReleaseChannel(managedCh)
	assert.NoError(t, err)
}

func TestChannelManagerReleaseChannelRequeue(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 2},
		channels:   make(chan *ManagedChannel, 2),
		closed:     false,
	}

	managedCh := &ManagedChannel{
		createdAt:   time.Now(),
		lastUsedAt:  time.Now(),
		isBroken:    false,
		manager:     cm,
		notifyClose: make(chan *amqp.Error, 1),
	}

	require.Equal(t, 0, len(cm.channels))
	err := cm.ReleaseChannel(managedCh)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cm.channels))
}

func TestChannelManagerCloseAlreadyClosed(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel),
		closed:     true,
	}

	err := cm.Close()
	assert.NoError(t, err)
}

func TestChannelManagerCloseWithChannels(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 3},
		channels:   make(chan *ManagedChannel, 3),
		closed:     false,
	}

	for range 3 {
		managedCh := &ManagedChannel{
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	err := cm.Close()
	assert.NoError(t, err)
	assert.True(t, cm.closed)
}

func TestChannelManagerGetStatsEmpty(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 5},
		channels:   make(chan *ManagedChannel, 5),
		closed:     false,
	}

	stats := cm.GetStats()
	assert.Equal(t, 0, stats.AvailableChannels)
	assert.Equal(t, 5, stats.MaxChannels)
	assert.False(t, stats.IsClosed)
}

func TestChannelManagerGetStatsWithChannels(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 5},
		channels:   make(chan *ManagedChannel, 5),
		closed:     false,
	}

	for i := 0; i < 3; i++ {
		managedCh := &ManagedChannel{
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	stats := cm.GetStats()
	assert.Equal(t, 3, stats.AvailableChannels)
	assert.Equal(t, 5, stats.MaxChannels)
	assert.False(t, stats.IsClosed)
}

func TestChannelManagerGetStatsClosed(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 5},
		channels:   make(chan *ManagedChannel, 5),
		closed:     true,
	}

	stats := cm.GetStats()
	assert.Equal(t, 5, stats.MaxChannels)
	assert.True(t, stats.IsClosed)
}

func TestChannelManagerAcquireChannelSuccessfully(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 2, ChannelTimeout: 5 * time.Second},
		channels:   make(chan *ManagedChannel, 2),
		closed:     false,
	}

	for range 2 {
		managedCh := &ManagedChannel{
			channel:     nil,
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	ch, err := cm.AcquireChannel(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, ch)
	assert.False(t, ch.IsBroken())
	assert.Equal(t, uint64(1), ch.usageCount)
}

func TestManagedChannelIsBrokenFalse(t *testing.T) {
	managedCh := &ManagedChannel{
		isBroken:   false,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}

	assert.False(t, managedCh.IsBroken())
}

func TestManagedChannelIsBrokenTrue(t *testing.T) {
	managedCh := &ManagedChannel{
		isBroken:   true,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}

	assert.True(t, managedCh.IsBroken())
}

func TestManagedChannelStats(t *testing.T) {
	now := time.Now()
	lastUsed := now.Add(1 * time.Second)

	managedCh := &ManagedChannel{
		createdAt:  now,
		lastUsedAt: lastUsed,
		usageCount: 42,
		isBroken:   false,
	}

	stats := managedCh.Stats()
	assert.Equal(t, now, stats.CreatedAt)
	assert.Equal(t, lastUsed, stats.LastUsedAt)
	assert.Equal(t, uint64(42), stats.UsageCount)
	assert.False(t, stats.IsBroken)
}

func TestManagedChannelStatsBroken(t *testing.T) {
	now := time.Now()

	managedCh := &ManagedChannel{
		createdAt:  now,
		lastUsedAt: now,
		usageCount: 10,
		isBroken:   true,
	}

	stats := managedCh.Stats()
	assert.True(t, stats.IsBroken)
	assert.Equal(t, uint64(10), stats.UsageCount)
}

func TestManagedChannelCloseNilChannel(t *testing.T) {
	managedCh := &ManagedChannel{
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		isBroken:   false,
	}

	err := managedCh.Close()
	assert.NoError(t, err)
}

func TestManagedChannelCloseBrokenChannel(t *testing.T) {
	managedCh := &ManagedChannel{
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		isBroken:   true,
	}

	err := managedCh.Close()
	assert.NoError(t, err)
}

func TestChannelManagerWithContextManagerClosed(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     true,
	}

	err := cm.WithContext(context.Background(), func(ch *amqp.Channel) error {
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, "channel manager is closed", err.Error())
}

func TestChannelManagerHealthCheckManagerClosed(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 1},
		channels:   make(chan *ManagedChannel, 1),
		closed:     true,
	}

	err := cm.HealthCheck(context.Background())
	assert.Error(t, err)
}

func TestChannelManagerConcurrencyAcquireRelease(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config: ChannelConfig{
			MaxChannels:    10,
			ChannelTimeout: 30 * time.Second,
		},
		channels: make(chan *ManagedChannel, 10),
		closed:   false,
	}

	for range 10 {
		managedCh := &ManagedChannel{
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ch, err := cm.AcquireChannel(context.Background())
			if err != nil {
				errors <- err
				return
			}

			time.Sleep(5 * time.Millisecond)

			if err := cm.ReleaseChannel(ch); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		assert.NoError(t, err)
	}

	stats := cm.GetStats()
	assert.Equal(t, 10, stats.AvailableChannels)
}

func TestChannelManagerConcurrencyGetStats(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 10},
		channels:   make(chan *ManagedChannel, 10),
		closed:     false,
	}

	for range 10 {
		managedCh := &ManagedChannel{
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	var wg sync.WaitGroup
	results := make(chan ChannelStats, 50)

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := cm.GetStats()
			results <- stats
		}()
	}

	wg.Wait()
	close(results)

	for stats := range results {
		assert.Equal(t, 10, stats.MaxChannels)
		assert.False(t, stats.IsClosed)
	}
}

func TestChannelManagerConcurrencyClose(t *testing.T) {
	cm := &ChannelManager{
		connection: &amqp.Connection{},
		config:     ChannelConfig{MaxChannels: 5},
		channels:   make(chan *ManagedChannel, 5),
		closed:     false,
	}

	for range 5 {
		managedCh := &ManagedChannel{
			createdAt:   time.Now(),
			lastUsedAt:  time.Now(),
			isBroken:    false,
			manager:     cm,
			notifyClose: make(chan *amqp.Error, 1),
		}
		cm.channels <- managedCh
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cm.Close()
			errors <- err
		}()
	}

	wg.Wait()
	close(errors)

	successCount := 0
	for err := range errors {
		if err == nil {
			successCount++
		}
	}

	assert.True(t, successCount > 0)
	assert.True(t, cm.closed)
}

func TestManagedChannelChannel(t *testing.T) {
	managedCh := &ManagedChannel{
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}

	result := managedCh.Channel()
	assert.Nil(t, result)
}
