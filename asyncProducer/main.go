package main

import (
	sarama "gopkg.in/Shopify/sarama.v1"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	kafkaBrokers = []string{"localhost:9092"}
	KafkaTopic   = "sarama_topic"
	enqueued     int
)

func main() {

	producer, err := setupProducer()
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}

	// Graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	produceMessages(producer, signals)

	log.Printf("Kafka AsyncProducer finished with %d messages produced.", enqueued)
}

func setupProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	return sarama.NewAsyncProducer(kafkaBrokers, config)
}

func produceMessages(producer sarama.AsyncProducer, signals chan os.Signal) {

	for {

		time.Sleep(time.Second)
		message := &sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.StringEncoder("testing 123")}

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
