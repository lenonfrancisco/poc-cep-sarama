package main

import (
	"fmt"
	"log"

	sarama "gopkg.in/Shopify/sarama.v1"
)

func main() {

	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.RequiredAcks(1)
	config.Producer.Compression = sarama.CompressionCodec(0)
	config.Producer.Retry.Max = 3
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true

	producer, err :=
		sarama.NewSyncProducer([]string{"localhost:9092"}, config)

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		sendMessage(producer, fmt.Sprintf("Message %d", i))
	}
}

func sendMessage(producer sarama.SyncProducer, value string) {

	msg := &sarama.ProducerMessage{Topic: "example", Value: sarama.StringEncoder(value)}
	partition, offset, err := producer.SendMessage(msg)

	if err != nil {

		log.Printf("FAILED to send message: %s\n", err)
		return
	}

	log.Printf("> message sent to partition %d at offset %d\n",
		partition, offset)
}
