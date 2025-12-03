package broker

// Example demonstrates how to use the broker abstraction with different providers.
// This shows the Open/Closed Principle in action - you can add new providers
// without modifying existing code.
//
// BASIC USAGE - Channel Pool:
//
//	func main() {
//	    // Initialize the factory with available providers
//	    factory := providers.InitializeFactory()
//
//	    // Create a RabbitMQ provider
//	    rabbitmqProvider, err := factory.CreateProvider(
//	        broker.ProviderRabbitMQ,
//	        "amqp://guest:guest@localhost:5672/",
//	    )
//	    if err != nil {
//	        panic(err)
//	    }
//
//	    // Connect to the broker
//	    ctx := context.Background()
//	    if err := rabbitmqProvider.Connect(ctx); err != nil {
//	        panic(err)
//	    }
//	    defer rabbitmqProvider.Disconnect(ctx)
//
//	    // Create a channel pool
//	    poolConfig := ChannelPoolConfig{
//	        MaxChannels:      10,
//	        ChannelTimeout:   10 * time.Second,
//	        ReconnectRetries: 3,
//	        RetryDelay:       500 * time.Millisecond,
//	    }
//
//	    pool, err := rabbitmqProvider.CreateChannelPool(poolConfig)
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer pool.Close()
//
//	    // Use the channel pool
//	    channel, err := pool.AcquireChannel(ctx)
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer pool.ReleaseChannel(channel)
//
//	    // Check pool statistics
//	    stats := pool.GetStats()
//	    fmt.Printf("Available channels: %d/%d\n",
//	        stats.AvailableChannels, stats.MaxChannels)
//	}
//
// TOPOLOGY MANAGEMENT - Queues, Exchanges, Bindings:
//
//	func setupTopology(ctx context.Context, pool broker.ChannelPool) error {
//	    // Create topology manager for RabbitMQ
//	    topoMgr := rabbit.NewTopologyManager(pool)
//
//	    // Declare a queue
//	    queueInfo, err := topoMgr.DeclareQueue(ctx, broker.QueueConfig{
//	        Name:       "orders.queue",
//	        Durable:    true,
//	        AutoDelete: false,
//	        Exclusive:  false,
//	        NoWait:     false,
//	        Arguments:  nil,
//	    })
//	    if err != nil {
//	        return err
//	    }
//	    fmt.Printf("Queue declared: %d messages, %d consumers\n",
//	        queueInfo["messages"], queueInfo["consumers"])
//
//	    // Declare an exchange
//	    err = topoMgr.DeclareExchange(ctx, broker.ExchangeConfig{
//	        Name:       "orders.exchange",
//	        Kind:       "topic",           // direct, topic, fanout, headers
//	        Durable:    true,
//	        AutoDelete: false,
//	        Internal:   false,
//	        NoWait:     false,
//	        Arguments:  nil,
//	    })
//	    if err != nil {
//	        return err
//	    }
//
//	    // Bind queue to exchange
//	    err = topoMgr.BindQueue(ctx, broker.BindingConfig{
//	        QueueName:    "orders.queue",
//	        ExchangeName: "orders.exchange",
//	        RoutingKey:   "order.*",
//	        NoWait:       false,
//	        Arguments:    nil,
//	    })
//	    if err != nil {
//	        return err
//	    }
//
//	    return nil
//	}
//
//	func cleanupTopology(ctx context.Context, topoMgr broker.TopologyManager) error {
//	    // Unbind queue before deletion
//	    if err := topoMgr.UnbindQueue(ctx, "orders.queue", "orders.exchange", "order.*"); err != nil {
//	        return err
//	    }
//
//	    // Delete queue
//	    if err := topoMgr.DeleteQueue(ctx, "orders.queue", false, false); err != nil {
//	        return err
//	    }
//
//	    // Delete exchange
//	    if err := topoMgr.DeleteExchange(ctx, "orders.exchange", false); err != nil {
//	        return err
//	    }
//
//	    return nil
//	}

// INTEGRATED EXAMPLE - Complete Workflow:
//
//	func completeExample(ctx context.Context) error {
//	    // 1. Create provider and connect
//	    factory := providers.InitializeFactory()
//	    provider, err := factory.CreateProvider(broker.ProviderRabbitMQ,
//	        "amqp://guest:guest@localhost:5672/")
//	    if err != nil {
//	        return err
//	    }
//
//	    if err := provider.Connect(ctx); err != nil {
//	        return err
//	    }
//	    defer provider.Disconnect(ctx)
//
//	    // 2. Create channel pool
//	    pool, err := provider.CreateChannelPool(broker.ChannelPoolConfig{
//	        MaxChannels:      20,
//	        ChannelTimeout:   10 * time.Second,
//	        ReconnectRetries: 3,
//	        RetryDelay:       500 * time.Millisecond,
//	    })
//	    if err != nil {
//	        return err
//	    }
//	    defer pool.Close()
//
//	    // 3. Setup topology (queues, exchanges, bindings)
//	    topoMgr := rabbit.NewTopologyManager(pool)
//	    if err := setupTopology(ctx, topoMgr); err != nil {
//	        return err
//	    }
//	    defer cleanupTopology(ctx, topoMgr)
//
//	    // 4. Use pool to acquire channels
//	    channel, err := pool.AcquireChannel(ctx)
//	    if err != nil {
//	        return err
//	    }
//	    defer pool.ReleaseChannel(channel)
//
//	    // 5. Check health
//	    if err := pool.HealthCheck(ctx); err != nil {
//	        return err
//	    }
//
//	    // 6. Inspect queue
//	    info, err := topoMgr.GetQueueInfo(ctx, "orders.queue")
//	    if err != nil {
//	        return err
//	    }
//	    fmt.Printf("Queue stats: %v\n", info)
//
//	    return nil
//	}
//
// MULTI-BROKER SUPPORT - Open/Closed Principle in Action:
//
// This demonstrates the Open/Closed Principle:
// - OPEN for extension: Add new broker providers (Kafka, NATS, etc.)
//   by implementing the BrokerProvider and TopologyManager interfaces
// - CLOSED for modification: Existing code using interfaces doesn't change
//   when new providers are added
//
// Example: Adding Kafka support would look like:
//
//	// kafka/provider.go
//	type KafkaProvider struct {
//	    client *kafka.Client
//	}
//
//	func (k *KafkaProvider) Connect(ctx context.Context) error {
//	    // Connect to Kafka cluster
//	}
//
//	func (k *KafkaProvider) CreateChannelPool(config broker.ChannelPoolConfig) (broker.ChannelPool, error) {
//	    // Create Kafka consumer group with connection pooling
//	}
//
//	// kafka/topology.go
//	type KafkaTopologyManager struct {
//	    admin kafka.Admin
//	}
//
//	func (k *KafkaTopologyManager) DeclareQueue(ctx context.Context, config broker.QueueConfig) (map[string]any, error) {
//	    // Create Kafka topic equivalent
//	}
//
//	func (k *KafkaTopologyManager) BindQueue(ctx context.Context, config broker.BindingConfig) error {
//	    // Setup consumer group subscription
//	}
//
//	// Then register in factory:
//	factory.Register(broker.ProviderKafka, func(url string) broker.BrokerProvider {
//	    return kafka.NewKafkaProvider(url)
//	})
