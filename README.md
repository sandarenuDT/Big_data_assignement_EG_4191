# Kafka Orders Processing System

A real-time order processing system built with Apache Kafka, featuring Avro serialization, Schema Registry integration, and automatic Dead Letter Queue (DLQ) handling.

## Features

- **Apache Kafka** running on WSL
- **Confluent Schema Registry** for schema management
- **Avro serialization** for efficient data encoding
- **Java-based** Producer and Consumer applications
- **Automatic DLQ handling** with single retry attempt
- **Running average calculation** for order prices

---

## Technologies Used

| Component | Technology |
|-----------|-----------|
| **Kafka Broker** | Apache Kafka 3.7 (WSL Manual Install) |
| **Schema Registry** | Confluent 7.3 |
| **Serialization** | Avro |
| **Language** | Java 17 |
| **Build Tool** | Maven |
| **DLQ Topic** | `orders-dlq` |

---

## ⚙️ Setup Instructions

### 1️⃣ Start Kafka Broker 

```bash
/mnt/c/kafka/kafka_2.13-3.7.0/bin/kafka-server-start.sh \
  /mnt/c/kafka/kafka_2.13-3.7.0/config/kraft/server.properties
```

### 2️⃣ Start Schema Registry 

```bash
/opt/confluent/bin/schema-registry-start \
  /opt/confluent/etc/schema-registry/schema-registry.properties
```

**Schema Registry URL:** `http://localhost:8081`

### 3️⃣ Create Kafka Topics

**Create `orders` topic:**

```bash
/mnt/c/kafka/kafka_2.13-3.7.0/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders \
  --partitions 3 --replication-factor 1
```

**Create `orders-dlq` topic:**

```bash
/mnt/c/kafka/kafka_2.13-3.7.0/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders-dlq \
  --partitions 1 --replication-factor 1
```

---

## Running the Application

### Start Order Consumer

```bash
mvn exec:java -Dexec.mainClass=com.assignment.kafka.OrderConsumer
```

### Start DLQ Consumer

```bash
mvn exec:java -Dexec.mainClass=com.assignment.kafka.DlqConsumer
```

### Start Order Producer

```bash
mvn exec:java -Dexec.mainClass=com.assignment.kafka.OrderProducer
```

---

## DLQ Logic

The consumer implements a **single retry policy**:

- **Success:** Message processed successfully
- **Failure (1 attempt):** Message immediately sent to `orders-dlq`
- **DLQ Consumer:** Reads and logs all failed messages

---

## Running Average Calculation

The consumer maintains a running average of order prices using the formula:

```java
runningAvg = runningAvg + (price - runningAvg) / count
```

This provides real-time insights into pricing trends without storing all historical data.

---

## ✅ Verification Checklist

- ✔️ Kafka running manually on WSL
- ✔️ Schema Registry running and accessible
- ✔️ Avro serialization functioning correctly
- ✔️ Producer successfully sending orders
- ✔️ Consumer processing orders and calculating running average
- ✔️ Failed messages automatically routed to DLQ
- ✔️ DLQ consumer reading and logging failed orders

---
