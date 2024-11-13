package consumer

import (
	"context"
	"log"
	"math/rand/v2"

	"github.com/IBM/sarama"
)

func New(ctx context.Context, brokers []string, topic string) error {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	if err := subscribe(ctx, topic, consumer); err != nil {
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

func subscribe(ctx context.Context, topic string, consumer sarama.Consumer) error {

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return err
	}

	pc, err := consumer.ConsumePartition(topic, partitions[rand.IntN(len(partitions))], sarama.OffsetOldest)
	if err != nil {
		return err
	}

	go func() {
		defer pc.Close()

		log.Printf("Subscribed to topic '%s'\n", topic)

		for msg := range pc.Messages() {
			if err := ctx.Err(); err != nil {
				log.Printf("Consumer cancelled: %v", err)
				return
			}
			log.Printf("Consumed message from topic '%s': Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		}

	}()

	return nil
}
