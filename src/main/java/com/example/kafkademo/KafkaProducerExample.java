package com.example.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerExample {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ObjectMapper mapper = new ObjectMapper();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                String key = "order-" + i;
                OrderEvent event = new OrderEvent(key, "Widget-" + i, i + 1);

                String jsonValue = mapper.writeValueAsString(event); // object -> JSON string

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("demo-topic", key, jsonValue);

                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Sent: key=%s value=%s -> partition=%d offset=%d%n",
                        key, jsonValue, metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//            for (int i = 0; i < 10; i++) {
//                String key = "key-" + i;
//                String value = "message number " + i;
//
//                ProducerRecord<String, String> record =
//                        new ProducerRecord<>("demo-topic", key, value);
//
//                RecordMetadata metadata = producer.send(record).get(); // sync send for demo purposes
//                System.out.printf("Sent: key=%s value=%s -> partition=%d offset=%d%n",
//                        key, value, metadata.partition(), metadata.offset());
//            }
//        }
    }
}