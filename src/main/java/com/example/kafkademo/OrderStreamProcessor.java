package com.example.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class OrderStreamProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-quantity-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        ObjectMapper mapper = new ObjectMapper();
        StreamsBuilder builder = new StreamsBuilder();

        // 1. Read raw JSON orders from the input topic
        KStream<String, String> orders = builder.stream("orders-topic");

        // 2. Re-key by product name, extract quantity, and sum continuously
        KTable<String, Long> totalsByProduct = orders
                .map((key, jsonValue) -> {
                    try {
                        OrderEvent event = mapper.readValue(jsonValue, OrderEvent.class);
                        return org.apache.kafka.streams.KeyValue.pair(event.getProduct(), (long) event.getQuantity());
                    } catch (Exception e) {
                        return org.apache.kafka.streams.KeyValue.pair("unparseable", 0L);
                    }
                })
                .groupByKey(org.apache.kafka.streams.kstream.Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum); // running total per product, updated on every new order

        // 3. Push the continuously-updated totals to an output topic
        totalsByProduct.toStream()
                .mapValues(String::valueOf)
                .to("product-totals-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close)); // clean shutdown on Ctrl+C

        System.out.println("Streaming app running... press Ctrl+C to stop.");
    }
}