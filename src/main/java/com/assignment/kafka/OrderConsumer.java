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
    private static final String DLQ = "orders-dlq";
    private static final String BOOTSTRAP = "localhost:9092";

    private static final Random RANDOM = new Random();

    private static double totalPrice = 0;
    private static int count = 0;

    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Properties dlqProps = new Properties();
        dlqProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, byte[]> dlqProducer = new KafkaProducer<>(dlqProps)) {

            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("OrderConsumer started.");

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, byte[]> rec : records) {

                    boolean processed = false;

                    for (int attempt = 1; attempt <= 3; attempt++) {
                        try {
                            process(rec);
                            processed = true;
                            break;
                        } catch (Exception ex) {
                            System.out.printf("FAILED key=%s attempt %d/3 → %s\n",
                                    rec.key(), attempt, ex.getMessage());
                        }
                    }

                    if (!processed) {
                        dlqProducer.send(new ProducerRecord<>(DLQ, rec.key(), rec.value()));
                        System.out.println("Moved to DLQ: " + rec.key());
                    }
                }

                consumer.commitSync();
            }
        }
    }

    private static void process(ConsumerRecord<String, byte[]> rec) {

        if (RANDOM.nextDouble() < 0.2) {
            throw new RuntimeException("Simulated failure");
        }

        GenericRecord order = AvroUtils.deserializeOrder(rec.value());
        float price = (Float) order.get("price");

        count++;
        totalPrice += price;

        System.out.printf("Processed %s → price=%.2f | runningAvg=%.2f\n",
                order.get("orderId"), price, totalPrice / count);
    }
}
