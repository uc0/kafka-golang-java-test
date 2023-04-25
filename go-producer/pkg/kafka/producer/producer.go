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

func (p *MyProducer) Close() {
	p.producer.Close()
}
