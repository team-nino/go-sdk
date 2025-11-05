package rabbit

import (
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

//TODO
//1. QueueDeclaring
//2. ExchangeDeclare
//3. Binding
//4. Add logger

type Connector interface {
	QueueDeclare(channel amqp.Channel)
}

type Resolver interface {
	Resolve() ([]string, error)
}

type Connection struct {
	resolver   Resolver
	connection *amqp.Connection
	amqpConfig amqp.Config

	ReconnectInterval time.Duration
	reconnectCount    uint

	connectionMu *sync.RWMutex
	reconnectMU  *sync.RWMutex
}

func dial(resolver Resolver, cfg amqp.Config) (*amqp.Connection, error) {
	urls, err := resolver.Resolve()
	if err != nil {
		return nil, fmt.Errorf("error resolving amqp server urls: %w", err)
	}

	var errs []error
	for _, url := range urls {
		conn, err := amqp.DialConfig(url, cfg)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}

	return nil, errors.Join(errs...)
}

func NewConnection(resolver Resolver, cfg amqp.Config, reconnectInterval time.Duration) (*Connection, error) {
	conn, err := dial(resolver, cfg)
	if err != nil {
		return nil, fmt.Errorf("error resolving amqp server urls: %w", err)
	}

	connection := Connection{
		resolver:          resolver,
		connection:        conn,
		amqpConfig:        cfg,
		reconnectCount:    0,
		ReconnectInterval: reconnectInterval,
		connectionMu:      &sync.RWMutex{},
		reconnectMU:       &sync.RWMutex{},
	}

	return &connection, nil
}
