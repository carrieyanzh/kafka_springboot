package com.example.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

public class FraudDetectionProcessor {

    private static final double LARGE_AMOUNT_THRESHOLD = 10000.00;
    private static final long VELOCITY_WINDOW_SECONDS = 5;
    private static final long VELOCITY_COUNT_THRESHOLD = 5;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ObjectMapper mapper = new ObjectMapper();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> transactions = builder.stream("transactions-topic");

        // --- Rule 1: flag any single large transaction immediately ---
        transactions
                .filter((accountId, json) -> {
                    try {
                        TransactionEvent tx = mapper.readValue(json, TransactionEvent.class);
                        return tx.getAmount() >= LARGE_AMOUNT_THRESHOLD;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .mapValues(json -> "ALERT: Large transaction -> " + json)
                .to("fraud-alerts-topic", Produced.with(Serdes.String(), Serdes.String()));

        // --- Rule 2: flag accounts with too many transactions in a short window (velocity check) ---
        transactions
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(VELOCITY_WINDOW_SECONDS), Duration.ofSeconds(1)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("velocity-store"))
                .toStream()
                .filter((windowedKey, count) -> count >= VELOCITY_COUNT_THRESHOLD)
                .map((windowedKey, count) -> org.apache.kafka.streams.KeyValue.pair(
                        windowedKey.key(),
                        "ALERT: Velocity fraud -> account=" + windowedKey.key() + " had " + count + " transactions in " + VELOCITY_WINDOW_SECONDS + "s"
                ))
                .to("fraud-alerts-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Fraud detection engine running... press Ctrl+C to stop.");
    }
}