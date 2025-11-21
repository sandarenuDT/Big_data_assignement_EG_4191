package com.assignment.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class OrderConsumer {

    private static final String TOPIC = "orders";
    private static final String DLQ_TOPIC = "orders-dlq";
    private static final String BOOTSTRAP = "localhost:9092";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {

        // Consumer Configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // DLQ Producer Configuration
        Properties dlqProps = new Properties();
        dlqProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        KafkaProducer<String, byte[]> dlqProducer = new KafkaProducer<>(dlqProps);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {

            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("OrderConsumer started.");

            float runningAvg = 0;
            int count = 0;

            while (true) {

                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, byte[]> record : records) {

                    GenericRecord order = AvroUtils.deserializeOrder(record.value());
                    String key = record.key();
                    float price = (float) order.get("price");

                    boolean processed = false;

                    for (int attempt = 1; attempt <= 1; attempt++) { // only 1 attempt
                        try {
                            // Random failure (50% chance)
                            if (RANDOM.nextDouble() < 0.5) {
                                throw new RuntimeException("Simulated failure");
                            }

                            // SUCCESS
                            count++;
                            runningAvg = runningAvg + (price - runningAvg) / count;

                            System.out.printf("Processed %s → %.2f | RunningAvg=%.2f%n",
                                    key, price, runningAvg);

                        } catch (Exception ex) {

                            System.err.printf("FAILED key=%s → %s%n", key, ex.getMessage());

                            // SEND TO DLQ IMMEDIATELY
                            dlqProducer.send(
                                    new ProducerRecord<>(DLQ_TOPIC, key, record.value()));

                            System.err.println("→ Sent to DLQ: " + key);
                        }
                    }

                }
            }
        }
    }
}
