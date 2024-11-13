package consumer

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Consumer struct {
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		fmt.Printf("Message claimed from topic '%s' partition %d offset %d\n",
			msg.Topic, msg.Partition, msg.Offset)
		fmt.Printf("Key: %s\nValue: %s\n", string(msg.Key), string(msg.Value))

		session.MarkMessage(msg, "")
	}

	return nil
}
