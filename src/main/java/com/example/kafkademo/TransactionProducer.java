package com.example.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class TransactionProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();
        String[] accounts = {"ACC-1001", "ACC-1002", "ACC-1003"};

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Simulate ACC-1001 being hit by rapid-fire transactions (velocity fraud pattern)
            for (int i = 0; i < 8; i++) {
                TransactionEvent tx = new TransactionEvent(
                        "TX-" + System.nanoTime(), "ACC-1001", 50 + random.nextInt(100), "WITHDRAWAL");
                sendTransaction(producer, mapper, tx);
                Thread.sleep(200); // 8 withdrawals in under 2 seconds — suspicious
            }

            // Simulate one large single transaction (amount fraud pattern)
            TransactionEvent bigTx = new TransactionEvent("TX-" + System.nanoTime(), "ACC-1002", 15000.00, "TRANSFER");
            sendTransaction(producer, mapper, bigTx);

            // Simulate normal, unremarkable activity
            TransactionEvent normalTx = new TransactionEvent("TX-" + System.nanoTime(), "ACC-1003", 45.50, "DEPOSIT");
            sendTransaction(producer, mapper, normalTx);
        }
    }

    private static void sendTransaction(KafkaProducer<String, String> producer, ObjectMapper mapper, TransactionEvent tx) throws Exception {
        String json = mapper.writeValueAsString(tx);
        producer.send(new ProducerRecord<>("transactions-topic", tx.getAccountId(), json));
        System.out.println("Sent: " + tx);
    }
}
