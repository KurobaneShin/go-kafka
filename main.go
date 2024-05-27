package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "something",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("failed to create producer: %s", err)
		os.Exit(1)
	}
	fmt.Printf("%+v\n", p)
}
