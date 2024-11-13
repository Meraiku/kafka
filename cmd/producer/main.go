package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"time"

	"github.com/meraiku/kafka/pkg/producer"
)

var (
	brokers = []string{"localhost:9095", "localhost:9096", "localhost:9097"}
)

func main() {
	sync, err := producer.NewSync(brokers)
	if err != nil {
		log.Fatalf("Fail to create sync producer: %v", err)
	}

	async, err := producer.NewAsync(brokers)
	if err != nil {
		log.Fatalf("Fail to create async producer: %v", err)
	}

	go func() {
		for err := range async.Errors() {
			log.Printf("Fail to send message async: %v", err)
		}
	}()

	go func() {
		for succ := range async.Successes() {
			log.Printf("Message send async. Topic: %s, Partition: %d, Offset: %d", succ.Topic, succ.Partition, succ.Offset)
		}
	}()

	for {
		user := CreateFakeUser()

		data, err := json.Marshal(user)
		if err != nil {
			log.Fatalf("Fail to marshal user: %v", err)
		}

		msg := producer.PrepareMessage("user", user.ID.String(), data)

		switch rand.IntN(2) {
		case 0:
			partition, offset, err := sync.SendMessage(msg)
			if err != nil {
				log.Printf("Fail to send message sync: %v", err)
			}

			log.Printf("Message send sync. Topic: %s, Partition: %d, Offset: %d", msg.Topic, partition, offset)

		case 1:
			async.Input() <- msg
		}

		time.Sleep(50 * time.Millisecond)
	}
}
