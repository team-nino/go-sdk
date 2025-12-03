package rabbit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

//#region ChannelManager

type ChannelConfig struct {
	MaxChannels      int
	ChannelTimeout   time.Duration
	ReconnectRetries int
	RetryDelay       time.Duration
}

type ChannelManager struct {
	connection *amqp.Connection
	config     ChannelConfig

	channels chan *ManagedChannel
	closed   bool

	mu sync.RWMutex
}

func NewChannelManager(connection *amqp.Connection, config ChannelConfig) (*ChannelManager, error) {
	if connection == nil {
		return nil, errors.New("connection cannot be nil")
	}

	if config.MaxChannels <= 0 {
		config.MaxChannels = 10
	}
	if config.ChannelTimeout == 0 {
		config.ChannelTimeout = 10 * time.Second
	}
	if config.ReconnectRetries <= 0 {
		config.ReconnectRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 500 * time.Millisecond
	}

	cm := &ChannelManager{
		connection: connection,
		config:     config,
		channels:   make(chan *ManagedChannel, config.MaxChannels),
		closed:     false,
	}

	for range config.MaxChannels {
		ch, err := cm.createChannel()
		if err != nil {
			return nil, fmt.Errorf("failed to create initial channel pool: %w", err)
		}
		cm.channels <- ch
	}

	return cm, nil
}

func (cm *ChannelManager) createChannel() (*ManagedChannel, error) {
	ch, err := cm.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	managed := &ManagedChannel{
		channel:     ch,
		createdAt:   time.Now(),
		lastUsedAt:  time.Now(),
		usageCount:  0,
		isBroken:    false,
		manager:     cm,
		notifyClose: ch.NotifyClose(make(chan *amqp.Error, 1)),
	}

	go managed.watchClose()

	return managed, nil
}

func (mc *ManagedChannel) watchClose() {
	<-mc.notifyClose
	mc.isBroken = true
}

func (cm *ChannelManager) AcquireChannel(ctx context.Context) (*ManagedChannel, error) {
	cm.mu.RLock()
	if cm.closed {
		cm.mu.RUnlock()
		return nil, errors.New("channel manager is closed")
	}
	cm.mu.RUnlock()

	select {
	case ch := <-cm.channels:
		if ch.isBroken {
			return cm.getOrCreateChannel(ctx)
		}
		ch.lastUsedAt = time.Now()
		ch.usageCount++
		return ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(cm.config.ChannelTimeout):
		return nil, errors.New("timeout waiting for available channel")
	}
}

func (cm *ChannelManager) getOrCreateChannel(ctx context.Context) (*ManagedChannel, error) {
	var lastErr error

	for attempt := range cm.config.ReconnectRetries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ch, err := cm.createChannel()
		if err == nil {
			return ch, nil
		}

		lastErr = err
		if attempt < cm.config.ReconnectRetries-1 {
			time.Sleep(cm.config.RetryDelay * time.Duration(attempt+1))
		}
	}

	return nil, fmt.Errorf("failed to create channel after %d retries: %w", cm.config.ReconnectRetries, lastErr)
}

func (cm *ChannelManager) ReleaseChannel(ch *ManagedChannel) error {
	if ch == nil {
		return errors.New("channel cannot be nil")
	}

	cm.mu.RLock()
	if cm.closed {
		cm.mu.RUnlock()
		return ch.Close()
	}
	cm.mu.RUnlock()

	if ch.isBroken {
		return ch.Close()
	}

	select {
	case cm.channels <- ch:
		return nil
	default:
		return ch.Close()
	}
}

func (cm *ChannelManager) Close() error {
	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return nil
	}
	cm.closed = true
	cm.mu.Unlock()

	close(cm.channels)

	var errs []error
	for ch := range cm.channels {
		if err := ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (cm *ChannelManager) GetStats() ChannelStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return ChannelStats{
		AvailableChannels: len(cm.channels),
		MaxChannels:       cm.config.MaxChannels,
		IsClosed:          cm.closed,
	}
}

type ChannelStats struct {
	AvailableChannels int
	MaxChannels       int
	IsClosed          bool
}

//#endregion

//#region ManagedChannel

type ManagedChannel struct {
	channel     *amqp.Channel
	createdAt   time.Time
	lastUsedAt  time.Time
	usageCount  uint64
	isBroken    bool
	manager     *ChannelManager
	notifyClose chan *amqp.Error
}
type ChannelInfo struct {
	CreatedAt  time.Time
	LastUsedAt time.Time
	UsageCount uint64
	IsBroken   bool
}

func (mc *ManagedChannel) Channel() *amqp.Channel {
	return mc.channel
}

func (mc *ManagedChannel) IsBroken() bool {
	return mc.isBroken
}

func (mc *ManagedChannel) Stats() ChannelInfo {
	return ChannelInfo{
		CreatedAt:  mc.createdAt,
		LastUsedAt: mc.lastUsedAt,
		UsageCount: mc.usageCount,
		IsBroken:   mc.isBroken,
	}
}

func (mc *ManagedChannel) Close() error {
	if mc.channel == nil || mc.isBroken {
		return nil
	}
	return mc.channel.Close()
}

func (cm *ChannelManager) WithContext(ctx context.Context, fn func(*amqp.Channel) error) error {
	ch, err := cm.AcquireChannel(ctx)
	if err != nil {
		return err
	}
	defer cm.ReleaseChannel(ch)

	return fn(ch.Channel())
}

func (cm *ChannelManager) HealthCheck(ctx context.Context) error {
	ch, err := cm.AcquireChannel(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: cannot acquire channel: %w", err)
	}
	defer cm.ReleaseChannel(ch)

	err = ch.Channel().ExchangeDeclarePassive("", "direct", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

//#endregion
