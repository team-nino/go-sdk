package rabbit

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/team-nino/go-sdk/infra/broker"
)

const (
	errFailedAcquireChannel  = "failed to acquire channel: %w"
	errInvalidChannelType    = "channel is not a RabbitMQ channel"
	errFailedDeclareQueue    = "failed to declare queue %s: %w"
	errFailedDeleteQueue     = "failed to delete queue %s: %w"
	errFailedDeclareExchange = "failed to declare exchange %s: %w"
	errFailedDeleteExchange  = "failed to delete exchange %s: %w"
	errFailedBindQueue       = "failed to bind queue %s to exchange %s: %w"
	errFailedUnbindQueue     = "failed to unbind queue %s from exchange %s: %w"
	errFailedPurgeQueue      = "failed to purge queue %s: %w"
	errFailedInspectQueue    = "failed to inspect queue %s: %w"
)

// TopologyManager implements broker.TopologyManager for RabbitMQ.
// It manages queue and exchange declarations, bindings, and deletions.
type TopologyManager struct {
	pool broker.ChannelPool
}

// NewTopologyManager creates a new RabbitMQ topology manager.
func NewTopologyManager(pool broker.ChannelPool) *TopologyManager {
	return &TopologyManager{
		pool: pool,
	}
}

// DeclareQueue implements broker.TopologyManager interface.
func (t *TopologyManager) DeclareQueue(ctx context.Context, config broker.QueueConfig) (map[string]any, error) {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return nil, err
	}
	defer t.releaseChannel(amqpCh)

	queue, err := amqpCh.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		amqp.Table(convertToAMQPTable(config.Arguments)),
	)
	if err != nil {
		return nil, fmt.Errorf(errFailedDeclareQueue, config.Name, err)
	}

	return map[string]any{
		"name":      queue.Name,
		"messages":  queue.Messages,
		"consumers": queue.Consumers,
	}, nil
}

// DeleteQueue implements broker.TopologyManager interface.
func (t *TopologyManager) DeleteQueue(ctx context.Context, queueName string, ifUnused, ifEmpty bool) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	_, err = amqpCh.QueueDelete(queueName, ifUnused, ifEmpty, false)
	if err != nil {
		return fmt.Errorf(errFailedDeleteQueue, queueName, err)
	}

	return nil
}

// DeclareExchange implements broker.TopologyManager interface.
func (t *TopologyManager) DeclareExchange(ctx context.Context, config broker.ExchangeConfig) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	err = amqpCh.ExchangeDeclare(
		config.Name,
		config.Kind,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		amqp.Table(convertToAMQPTable(config.Arguments)),
	)
	if err != nil {
		return fmt.Errorf(errFailedDeclareExchange, config.Name, err)
	}

	return nil
}

// DeleteExchange implements broker.TopologyManager interface.
func (t *TopologyManager) DeleteExchange(ctx context.Context, exchangeName string, ifUnused bool) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	err = amqpCh.ExchangeDelete(exchangeName, ifUnused, false)
	if err != nil {
		return fmt.Errorf(errFailedDeleteExchange, exchangeName, err)
	}

	return nil
}

// BindQueue implements broker.TopologyManager interface.
func (t *TopologyManager) BindQueue(ctx context.Context, config broker.BindingConfig) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	err = amqpCh.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.ExchangeName,
		config.NoWait,
		amqp.Table(convertToAMQPTable(config.Arguments)),
	)
	if err != nil {
		return fmt.Errorf(errFailedBindQueue, config.QueueName, config.ExchangeName, err)
	}

	return nil
}

// UnbindQueue implements broker.TopologyManager interface.
func (t *TopologyManager) UnbindQueue(ctx context.Context, queueName, exchangeName, routingKey string) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	err = amqpCh.QueueUnbind(queueName, routingKey, exchangeName, nil)
	if err != nil {
		return fmt.Errorf(errFailedUnbindQueue, queueName, exchangeName, err)
	}

	return nil
}

// PurgeQueue implements broker.TopologyManager interface.
func (t *TopologyManager) PurgeQueue(ctx context.Context, queueName string) error {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return err
	}
	defer t.releaseChannel(amqpCh)

	_, err = amqpCh.QueuePurge(queueName, false)
	if err != nil {
		return fmt.Errorf(errFailedPurgeQueue, queueName, err)
	}

	return nil
}

// GetQueueInfo implements broker.TopologyManager interface.
func (t *TopologyManager) GetQueueInfo(ctx context.Context, queueName string) (map[string]any, error) {
	amqpCh, err := t.getChannel(ctx)
	if err != nil {
		return nil, err
	}
	defer t.releaseChannel(amqpCh)

	queue, err := amqpCh.QueueInspect(queueName)
	if err != nil {
		return nil, fmt.Errorf(errFailedInspectQueue, queueName, err)
	}

	return map[string]any{
		"name":      queue.Name,
		"messages":  queue.Messages,
		"consumers": queue.Consumers,
	}, nil
}

// Private helper methods

// getChannel acquires a channel from the pool and extracts the underlying AMQP channel.
func (t *TopologyManager) getChannel(ctx context.Context) (*amqp.Channel, error) {
	ch, err := t.pool.AcquireChannel(ctx)
	if err != nil {
		return nil, fmt.Errorf(errFailedAcquireChannel, err)
	}

	amqpCh := extractAMQPChannel(ch)
	if amqpCh == nil {
		return nil, errors.New(errInvalidChannelType)
	}

	return amqpCh, nil
}

// releaseChannel returns a channel to the pool.
func (t *TopologyManager) releaseChannel(ch broker.Channel) {
	_ = t.pool.ReleaseChannel(ch)
}

// extractAMQPChannel extracts the underlying AMQP channel from the broker.Channel interface.
func extractAMQPChannel(ch broker.Channel) *amqp.Channel {
	if adapter, ok := ch.(interface{ GetRawChannel() *amqp.Channel }); ok {
		return adapter.GetRawChannel()
	}
	return nil
}

// convertToAMQPTable converts map[string]any to amqp.Table.
func convertToAMQPTable(args map[string]any) amqp.Table {
	if args == nil {
		return amqp.Table{}
	}
	return amqp.Table(args)
}
