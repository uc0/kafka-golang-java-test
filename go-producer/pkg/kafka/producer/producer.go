package producer

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
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
	schemaRegistry schemaregistry.Client
}

func Get() *MyProducer {
	if prod == nil || prod.producer == nil || prod.schemaRegistry == nil {
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

	schemaRegistryServer := os.Getenv("SCHEMA_REGISTRY_SERVER")
	schemaRegistry, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryServer))
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s\n", err)
	}
	log.Printf("Created SCHEMA REGISTRY %v\n", schemaRegistry)

	p.producer = producer
	p.schemaRegistry = schemaRegistry
}

func (p *MyProducer) SendMessage(topic string, message interface{}) (err error) {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	ser, err := protobuf.NewSerializer(p.schemaRegistry, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create serializer: %s\n", err))
	}

	payload, err := ser.Serialize(topic, message)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to serialize payload: %s\n", err))
	}


	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          payload,
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
