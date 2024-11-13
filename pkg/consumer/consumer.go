package consumer

import (
	"context"
	"log"
	"time"

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

func NewGroup(
	ctx context.Context,
	brokers []string,
	groupID, topic string,
) error {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 100 * time.Millisecond

	config.Consumer.MaxWaitTime = 500 * time.Millisecond

	config.Consumer.Return.Errors = true

	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return err
	}

	if err := subscribeGroup(ctx, topic, consumer); err != nil {
		return err
	}

	return nil
}

func subscribeGroup(
	ctx context.Context,
	topic string,
	consumer sarama.ConsumerGroup,
) error {

	handler := &Consumer{}

	go func() {
		defer consumer.Close()

		for {
			err := consumer.Consume(ctx, []string{topic}, handler)
			if err != nil {
				log.Printf("Error from consumer: %v", err)
				log.Println("Closing consumer...")
				return
			}
		}
	}()

	return nil
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
