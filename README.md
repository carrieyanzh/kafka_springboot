# Kafka Spring Boot Demo

A hands-on Kafka learning project built on Windows with Docker, covering the core producer/consumer API, Kafka Streams aggregation, and a real-time fraud detection pipeline.

## Overview

This repo demonstrates three progressively advanced Kafka patterns using plain Java (`kafka-clients` and `kafka-streams`), all running against a single-node Kafka broker in Docker:

1. **Basic Producer/Consumer** — fundamental message send/receive with JSON serialization
2. **Kafka Streams Aggregation** — real-time running totals using stateful stream processing
3. **Fraud Detection Pipeline** — windowed aggregation and rule-based alerting, modeled on a banking use case

## Architecture

```
                        ┌─────────────────────────┐
                        │   Kafka Broker (Docker) │
                        │      localhost:9092     │
                        └─────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
   Example 1                   Example 2                   Example 3
   ─────────                   ─────────                   ─────────
KafkaProducerExample      TransactionProducer         TransactionProducer
        │                           │                           │
        ▼                           ▼                           ▼
  demo-topic               orders-topic              transactions-topic
        │                           │                           │
        ▼                           ▼                           ▼
KafkaConsumerExample      OrderStreamProcessor      FraudDetectionProcessor
                          (Kafka Streams app)         (Kafka Streams app)
                                    │                           │
                                    ▼                           ▼
                        product-totals-topic         fraud-alerts-topic
                                    │                           │
                                    ▼                           ▼
                          console-consumer              console-consumer
```

**Design notes:**
- Examples 1–3 share the same Kafka broker (single Docker container, `KRaft` mode — no separate Zookeeper needed).
- Messages are serialized as JSON strings using Jackson (`ObjectMapper`), keeping serialization logic explicit in application code rather than hidden inside custom Kafka `Serializer` classes — intentionally simple for learning purposes.
- Examples 2 and 3 use **Kafka Streams**, which differs from a plain consumer: it maintains internal state (via local state stores + Kafka changelog topics) and continuously re-emits updated results as new data arrives, rather than processing each message once and discarding it.
- Example 3 combines a **stateless** rule (large-transaction filter) with a **stateful, time-windowed** rule (transaction velocity count), mirroring how real fraud-detection systems layer multiple checks on the same live stream.

## Prerequisites

- Docker Desktop (Windows)
- Java 17+
- Maven
- IntelliJ IDEA (or any IDE)

## Docker Setup

**`docker-compose.yml`** (single-node Kafka broker, KRaft mode):

```yaml
version: '3.8'
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

**Start the broker:**

```cmd
docker compose up -d
```

**Verify it's running:**

```cmd
docker ps
```

---

## Example 1: Basic Producer/Consumer (JSON)

Sends `OrderEvent` objects as JSON strings to `demo-topic`, and consumes/deserializes them back into objects.

**Create the topic:**

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic demo-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Run order:**

1. Run `KafkaConsumerExample.java` (starts polling, leave it running)
2. Run `KafkaProducerExample.java` (sends 10 messages, then exits)

Expected consumer output:
```
Received: partition=1 offset=0 key=order-0 event=OrderEvent{orderId='order-0', product='Widget-0', quantity=1}
```

---

## Example 2: Kafka Streams — Real-Time Order Totals

Continuously aggregates total quantity sold per product as orders arrive, using `KTable` reduce.

**Create the topics:**

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic product-totals-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Run order (Streams app must start first):**

1. Run `OrderStreamProcessor.java` — leave running (`Streaming app running... press Ctrl+C to stop.`)
2. Start the console consumer:
   ```cmd
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic product-totals-topic --property print.key=true --from-beginning
   ```
3. Run `KafkaProducerExample.java` to send orders

Expected output in the console consumer:
```
Widget-0    1
Widget-1    2
Widget-0    5
```

---

## Example 3: Banking Fraud Detection Pipeline

Detects two fraud patterns in real time:
- **Large transaction** — single transaction over $10,000
- **Velocity fraud** — 5+ transactions from the same account within a 5-second window

**Create the topics:**

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic fraud-alerts-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Run order (Streams app must start first):**

1. Run `FraudDetectionProcessor.java` — leave running (`Fraud detection engine running... press Ctrl+C to stop.`)
2. Start the console consumer:
   ```cmd
   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud-alerts-topic --property print.key=true --from-beginning
   ```
3. Run `TransactionProducer.java` to simulate transactions

Expected output in the console consumer:
```
ACC-1001    ALERT: Velocity fraud -> account=ACC-1001 had 5 transactions in 5s
ACC-1002    ALERT: Large transaction -> {"transactionId":"TX-...","accountId":"ACC-1002","amount":15000.0,"type":"TRANSFER"}
```

Note: `ACC-1003` (a single small deposit) correctly triggers no alert.

---

## Project Structure

```
kafkademo/
├── docker-compose.yml
├── pom.xml
├── src/main/java/
│   ├── OrderEvent.java
│   ├── KafkaProducerExample.java
│   ├── KafkaConsumerExample.java
│   ├── OrderStreamProcessor.java
│   ├── TransactionEvent.java
│   ├── TransactionProducer.java
│   └── FraudDetectionProcessor.java
└── README.md
```

## Common Issues (Windows)

- **`ADVERTISED_LISTENERS` mismatch:** Must be `localhost:9092` (not `kafka:9092`), since Java apps connect from the Windows host, not from inside the Docker network.
- **No output on console consumer:** Confirm the relevant Kafka Streams app (`OrderStreamProcessor` or `FraudDetectionProcessor`) is actually running *before* the producer sends messages — the console consumer only shows data that has flowed through the Streams transformation.
- **Reprocessing old messages:** If you sent producer messages before starting a Streams app, reset its offsets to reprocess:
  ```cmd
  docker exec -it kafka /opt/kafka/bin/kafka-streams-application-reset.sh --application-id <application-id> --input-topics <input-topic> --bootstrap-server localhost:9092
  ```
