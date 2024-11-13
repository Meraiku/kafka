package consumer

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

func NewSingle(ctx context.Context, brokers []string, topic string) error {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	if err := subscribeAll(ctx, topic, consumer); err != nil {
		return err
	}

	return nil
}

func NewGroup(brokers []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func subscribeAll(ctx context.Context, topic string, consumer sarama.Consumer) error {

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return err
		}

		go func() {
			defer pc.Close()

			log.Printf("Subscribed to topic '%s' on partition '%d'\n", topic, partition)

			for msg := range pc.Messages() {
				if err := ctx.Err(); err != nil {
					log.Printf("Consumer cancelled: %v", err)
					return
				}
				log.Printf("Consumed message from topic '%s': Partition: %d, Offset: %d, Key: %s, Value: %s\n",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}

		}()
	}

	return nil
}
