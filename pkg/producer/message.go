package producer

import "github.com/IBM/sarama"

func PrepareMessage(topic, key string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(message),
		Partition: -1,
	}

	return msg
}
