package providers

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/team-nino/go-sdk/infra/broker"
	"github.com/team-nino/go-sdk/infra/broker/rabbit"
)

type ManagedChannelAdapter struct {
	managedCh *rabbit.ManagedChannel
}

func NewManagedChannelAdapter(ch *rabbit.ManagedChannel) *ManagedChannelAdapter {
	return &ManagedChannelAdapter{managedCh: ch}
}

func (m *ManagedChannelAdapter) Close() error {
	return m.managedCh.Close()
}

// IsClosed implements broker.Channel interface.
func (m *ManagedChannelAdapter) IsClosed() bool {
	return m.managedCh.IsBroken()
}

// GetRawChannel returns the underlying AMQP channel for RabbitMQ-specific operations.
func (m *ManagedChannelAdapter) GetRawChannel() *amqp.Channel {
	return m.managedCh.Channel()
}

// ChannelPoolAdapter adapts RabbitMQ ChannelManager to the broker.ChannelPool interface.
type ChannelPoolAdapter struct {
	manager *rabbit.ChannelManager
}

// NewChannelPoolAdapter creates a new adapter for a ChannelManager.
func NewChannelPoolAdapter(manager *rabbit.ChannelManager) *ChannelPoolAdapter {
	return &ChannelPoolAdapter{manager: manager}
}

// AcquireChannel implements broker.ChannelPool interface.
func (c *ChannelPoolAdapter) AcquireChannel(ctx context.Context) (broker.Channel, error) {
	managedCh, err := c.manager.AcquireChannel(ctx)
	if err != nil {
		return nil, err
	}
	return NewManagedChannelAdapter(managedCh), nil
}

// ReleaseChannel implements broker.ChannelPool interface.
func (c *ChannelPoolAdapter) ReleaseChannel(ch broker.Channel) error {
	adapter, ok := ch.(*ManagedChannelAdapter)
	if !ok {
		return ch.Close()
	}
	return c.manager.ReleaseChannel(adapter.managedCh)
}

// Close implements broker.ChannelPool interface.
func (c *ChannelPoolAdapter) Close() error {
	return c.manager.Close()
}

// GetStats implements broker.ChannelPool interface.
func (c *ChannelPoolAdapter) GetStats() broker.PoolStats {
	stats := c.manager.GetStats()
	return broker.PoolStats{
		AvailableChannels: stats.AvailableChannels,
		MaxChannels:       stats.MaxChannels,
		IsClosed:          stats.IsClosed,
	}
}

// HealthCheck implements broker.ChannelPool interface.
func (c *ChannelPoolAdapter) HealthCheck(ctx context.Context) error {
	return c.manager.HealthCheck(ctx)
}

// RabbitMQProvider implements broker.BrokerProvider for RabbitMQ.
type RabbitMQProvider struct {
	connection *amqp.Connection
	url        string
	connected  bool
}

// NewRabbitMQProvider creates a new RabbitMQ broker provider.
func NewRabbitMQProvider(url string) *RabbitMQProvider {
	return &RabbitMQProvider{
		url:       url,
		connected: false,
	}
}

// Connect implements broker.BrokerProvider interface.
func (r *RabbitMQProvider) Connect(ctx context.Context) error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}
	r.connection = conn
	r.connected = true
	return nil
}

// Disconnect implements broker.BrokerProvider interface.
func (r *RabbitMQProvider) Disconnect(ctx context.Context) error {
	if r.connection != nil {
		r.connected = false
		return r.connection.Close()
	}
	return nil
}

// CreateChannelPool implements broker.BrokerProvider interface.
func (r *RabbitMQProvider) CreateChannelPool(config broker.ChannelPoolConfig) (broker.ChannelPool, error) {
	if !r.connected || r.connection == nil {
		return nil, errors.New("rabbitmq connection is closed")
	}

	rmqConfig := rabbit.ChannelConfig{
		MaxChannels:      config.MaxChannels,
		ReconnectRetries: config.ReconnectRetries,
	}

	if td, ok := config.ChannelTimeout.(time.Duration); ok {
		rmqConfig.ChannelTimeout = td
	}
	if rd, ok := config.RetryDelay.(time.Duration); ok {
		rmqConfig.RetryDelay = rd
	}

	manager, err := rabbit.NewChannelManager(r.connection, rmqConfig)
	if err != nil {
		return nil, err
	}

	return NewChannelPoolAdapter(manager), nil
}

// IsConnected implements broker.BrokerProvider interface.
func (r *RabbitMQProvider) IsConnected() bool {
	return r.connected && r.connection != nil && !r.connection.IsClosed()
}

// GetProviderName implements broker.BrokerProvider interface.
func (r *RabbitMQProvider) GetProviderName() string {
	return "rabbitmq"
}
