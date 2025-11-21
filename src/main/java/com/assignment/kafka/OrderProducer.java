package com.assignment.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class OrderProducer {

    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP = "localhost:9092";

    private static final String[] PRODUCTS = {"Item1", "Item2", "Item3", "Item4", "Item5"};
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {

            for (int i = 1; i <= 20; i++) {

                String orderId = "100" + i;
                String product = PRODUCTS[RANDOM.nextInt(PRODUCTS.length)];
                float price = 20 + RANDOM.nextFloat() * 80;

                byte[] value = AvroUtils.serializeOrder(orderId, product, price);

                ProducerRecord<String, byte[]> record =
                        new ProducerRecord<>(TOPIC, orderId, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent order %s → %s (%.2f)%n",
                                orderId, product, price);
                    } else {
                        System.err.println("Producer failed: " + exception.getMessage());
                    }
                });

                Thread.sleep(200);
            }

            System.out.println("Finished sending orders.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
