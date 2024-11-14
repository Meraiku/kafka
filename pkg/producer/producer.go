package producer

import (
	"github.com/IBM/sarama"
)

func NewSync(brokers []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()

	config.Producer.Partitioner = sarama.NewRandomPartitioner

	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func NewAsync(brokers []string) (sarama.AsyncProducer, error) {

	config := sarama.NewConfig()

	config.Producer.Partitioner = sarama.NewRandomPartitioner

	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1

	// Example for correct transaction implementation for Exactly Once
	config.Producer.Transaction.ID = "prefix.group_id.topic.paritition"
	config.Consumer.IsolationLevel = sarama.ReadCommitted

	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
