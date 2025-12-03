package providers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/team-nino/go-sdk/infra/broker"
)

func TestRabbitMQProviderImplementsBrokerProvider(t *testing.T) {
	// This test verifies that RabbitMQProvider correctly implements the BrokerProvider interface
	var _ broker.BrokerProvider = (*RabbitMQProvider)(nil)
}

func TestChannelPoolAdapterImplementsChannelPool(t *testing.T) {
	// This test verifies that ChannelPoolAdapter correctly implements the ChannelPool interface
	var _ broker.ChannelPool = (*ChannelPoolAdapter)(nil)
}

func TestManagedChannelAdapterImplementsChannel(t *testing.T) {
	// This test verifies that ManagedChannelAdapter correctly implements the Channel interface
	var _ broker.Channel = (*ManagedChannelAdapter)(nil)
}

func TestFactoryCanCreateRabbitMQProvider(t *testing.T) {
	// Initialize factory with provider
	factory := InitializeFactory()

	// Create a RabbitMQ provider
	provider, err := factory.CreateProvider(
		broker.ProviderRabbitMQ,
		"amqp://guest:guest@localhost:5672/",
	)

	assert.NoError(t, err)
	assert.NotNil(t, provider)
	assert.Equal(t, "rabbitmq", provider.GetProviderName())
	assert.False(t, provider.IsConnected())
}

func TestFactoryReturnsErrorForUnsupportedProvider(t *testing.T) {
	factory := InitializeFactory()

	// Try to create an unsupported provider
	provider, err := factory.CreateProvider(
		broker.ProviderKafka,
		"kafka://localhost:9092",
	)

	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestRabbitMQProviderGetSupportedProviders(t *testing.T) {
	factory := InitializeFactory()
	providers := factory.GetSupportedProviders()

	assert.NotEmpty(t, providers)
	assert.Contains(t, providers, broker.ProviderRabbitMQ)
}
