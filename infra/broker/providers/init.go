package providers

import "github.com/team-nino/go-sdk/infra/broker"

func InitializeFactory() *broker.Factory {
	factory := broker.NewFactory()

	factory.Register(broker.ProviderRabbitMQ, func(url string) broker.BrokerProvider {
		return NewRabbitMQProvider(url)
	})

	// !Kafka provider registration can be added here when implemented
	// factory.Register(broker.ProviderKafka, func(url string) broker.BrokerProvider {
	//     return NewKafkaProvider(url)
	// })

	return factory
}
