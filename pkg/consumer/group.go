package consumer

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Consumer struct {
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer Group are been rebalanced")
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer Group will be rebalanced soon!")
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		if err := session.Context().Err(); err != nil {
			fmt.Printf("Consumer cancelled: %v", err)
			return err
		}

		fmt.Printf("Message claimed from topic '%s' partition %d offset %d\n",
			msg.Topic, msg.Partition, msg.Offset)
		fmt.Printf("Key: %s\nValue: %s\n", string(msg.Key), string(msg.Value))

		session.MarkMessage(msg, "")
	}

	return nil
}
