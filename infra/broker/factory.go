package broker

import "fmt"

// ProviderType defines the supported broker providers.
type ProviderType string

const (
	ProviderRabbitMQ ProviderType = "rabbitmq"
	ProviderKafka    ProviderType = "kafka"
)

// Factory creates broker provider instances based on the provider type.
type Factory struct {
	providers map[ProviderType]func(string) BrokerProvider
}

// NewFactory creates a new broker provider factory.
func NewFactory() *Factory {
	return &Factory{
		providers: make(map[ProviderType]func(string) BrokerProvider),
	}
}

// Register registers a new provider factory function.
func (f *Factory) Register(providerType ProviderType, factory func(string) BrokerProvider) {
	f.providers[providerType] = factory
}

// CreateProvider creates a new broker provider instance.
func (f *Factory) CreateProvider(providerType ProviderType, url string) (BrokerProvider, error) {
	factory, exists := f.providers[providerType]
	if !exists {
		return nil, fmt.Errorf("unsupported broker provider: %s", providerType)
	}

	return factory(url), nil
}

// GetSupportedProviders returns a list of supported provider types.
func (f *Factory) GetSupportedProviders() []ProviderType {
	providers := make([]ProviderType, 0, len(f.providers))
	for p := range f.providers {
		providers = append(providers, p)
	}
	return providers
}
