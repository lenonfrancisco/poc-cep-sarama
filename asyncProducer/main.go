package main

import (
	sarama "gopkg.in/Shopify/sarama.v1"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	enqueued int
)

func main() {

	config := sarama.NewConfig()

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// Graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	sendMessage(producer, signals)

}

func sendMessage(producer sarama.AsyncProducer, signals chan os.Signal) {

	for {

		time.Sleep(time.Second)
		message := &sarama.ProducerMessage{Topic: "example", Value: sarama.StringEncoder("event message ...")}

		select {
		case producer.Input() <- message:
			enqueued++
			log.Println("New Message produced")
		case <-signals:
			producer.AsyncClose()
			return
		}
	}
}
