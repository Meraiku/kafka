package main

import (
	"context"
	"fmt"

	"github.com/meraiku/kafka/pkg/consumer"
)

var (
	brokers = []string{"localhost:9095", "localhost:9096", "localhost:9097"}
	topic   = "user"
)

func main() {
	ctx := context.TODO()

	if err := consumer.NewSingle(ctx, brokers, topic); err != nil {
		fmt.Printf("Fail to create consumer: %v", err)
	}

	for {
	}
}
