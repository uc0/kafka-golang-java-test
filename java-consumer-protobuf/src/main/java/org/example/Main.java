package org.example;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.protobuf.Test;
import org.example.protobuf.Test2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("JAVA KAFKA CONSUMER RUNNING...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-cg");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (
                KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props)
        ) {
            consumer.subscribe(List.of("my-test-topic", "my-test-2-topic"));

            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, Message> record : records) {
                    logger.info("CONSUME MESSAGE IN TOPIC: " + record.topic());

                    if (record.topic().equals("my-test-topic")) {
                        Test.TestMessage msg = Test.TestMessage.parseFrom(record.value().toByteArray());
                        logger.info("Title: " + msg.getTitle());
                        logger.info("Content: " + msg.getContent());
                        logger.info("Timestamp: " + msg.getTimestamp());
                    } else if (record.topic().equals("my-test-2-topic")) {
                        Test2.Test2Message msg = Test2.Test2Message.parseFrom(record.value().toByteArray());
                        logger.info("Message: " + msg.getMessage());
                        logger.info("MessageNumber: " + msg.getMessageNumber());
                        logger.info("Timestamp: " + msg.getTimestamp());
                    }

                    logger.info("CONSUME MESSAGE END\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

