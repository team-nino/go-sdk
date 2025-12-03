package broker

import "context"

// Channel represents a generic broker channel abstraction.
// Different implementations (RabbitMQ, Kafka) can adapt their specific channel types.
type Channel interface {
	Close() error
	IsClosed() bool
}

// ChannelPool manages a pool of channels for a broker connection.
// It handles channel acquisition, release, recycling, and health monitoring.
type ChannelPool interface {
	AcquireChannel(ctx context.Context) (Channel, error)
	ReleaseChannel(ch Channel) error
	Close() error
	GetStats() PoolStats
	HealthCheck(ctx context.Context) error
}

// PoolStats contains statistics about a channel pool's operational state.
type PoolStats struct {
	AvailableChannels int
	MaxChannels       int
	IsClosed          bool
}

// BrokerProvider defines the interface for broker implementations.
// Each provider (RabbitMQ, Kafka, etc.) implements these methods.
type BrokerProvider interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	CreateChannelPool(config ChannelPoolConfig) (ChannelPool, error)
	IsConnected() bool
	GetProviderName() string
}

// ChannelPoolConfig holds configuration for creating a channel pool.
type ChannelPoolConfig struct {
	MaxChannels      int
	ChannelTimeout   any
	ReconnectRetries int
	RetryDelay       any
	Options          map[string]any
}

// PublisherConfig holds configuration for message publishing.
type PublisherConfig struct {
	Confirmations bool
	Compression   string
	Options       map[string]any
}

// SubscriberConfig holds configuration for message subscription.
type SubscriberConfig struct {
	QoS     int
	AutoAck bool
	Options map[string]any
}

// Message represents a generic broker message.
type Message interface {
	GetBody() []byte
	GetHeaders() map[string]any
	Ack() error
	Nack(requeue bool) error
}

// Publisher defines the interface for publishing messages.
type Publisher interface {
	Publish(ctx context.Context, target string, message []byte, headers map[string]any) error
	Close() error
}

// Subscriber defines the interface for consuming messages.
type Subscriber interface {
	Subscribe(ctx context.Context, target string, handler func(Message) error) error
	Unsubscribe(target string) error
	Close() error
}

// ConnectionConfig holds broker connection configuration.
type ConnectionConfig struct {
	URL            string
	Username       string
	Password       string
	VirtualHost    string
	Heartbeat      any
	ConnectionName string
	Options        map[string]any
}

// QueueConfig holds configuration for queue declaration.
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  map[string]any
}

// ExchangeConfig holds configuration for exchange declaration.
type ExchangeConfig struct {
	Name       string
	Kind       string // direct, fanout, topic, headers
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]any
}

// BindingConfig holds configuration for queue-to-exchange binding.
type BindingConfig struct {
	QueueName    string
	ExchangeName string
	RoutingKey   string
	NoWait       bool
	Arguments    map[string]any
}

// TopologyManager manages broker topology (queues, exchanges, bindings).
// It provides a unified interface for declaring and managing broker resources.
type TopologyManager interface {
	// DeclareQueue creates a queue if it doesn't exist.
	// Returns queue properties or error.
	DeclareQueue(ctx context.Context, config QueueConfig) (map[string]any, error)

	// DeleteQueue removes a queue.
	DeleteQueue(ctx context.Context, queueName string, ifUnused, ifEmpty bool) error

	// DeclareExchange creates an exchange if it doesn't exist.
	DeclareExchange(ctx context.Context, config ExchangeConfig) error

	// DeleteExchange removes an exchange.
	DeleteExchange(ctx context.Context, exchangeName string, ifUnused bool) error

	// BindQueue binds a queue to an exchange with a routing key.
	BindQueue(ctx context.Context, config BindingConfig) error

	// UnbindQueue removes the binding between a queue and exchange.
	UnbindQueue(ctx context.Context, queueName, exchangeName, routingKey string) error

	// PurgeQueue removes all messages from a queue.
	PurgeQueue(ctx context.Context, queueName string) error

	// GetQueueInfo retrieves information about a queue.
	GetQueueInfo(ctx context.Context, queueName string) (map[string]any, error)
}
