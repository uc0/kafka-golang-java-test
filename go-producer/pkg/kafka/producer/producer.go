package producer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"sync"
)

var (
	prod *MyProducer
	once sync.Once
)

type MyProducer struct {
	producer *kafka.Producer
}

func Get() *MyProducer {
	if prod == nil || prod.producer == nil {
		once.Do(func() {
			prod = &MyProducer{}
			prod.init()
		})
	}
	return prod
}

func (p *MyProducer) init() {
	kafkaServer := os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServer})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	log.Printf("Created Producer %v\n", producer)

	p.producer = producer
}

func (p *MyProducer) SendMessage(topic, message string) (err error) {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          []byte(message),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		log.Printf("failed to produce: %v\n", err.Error())
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("failed to produce: %v\n", m.TopicPartition.Error.Error())
		return
	}

	log.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	return
}

func (p *MyProducer) Close() {
	p.producer.Close()
}
