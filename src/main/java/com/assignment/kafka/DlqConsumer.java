package com.assignment.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DlqConsumer {

    private static final String DLQ = "orders-dlq";
    private static final String BOOTSTRAP = "localhost:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(DLQ));
            System.out.println("DLQ Consumer started.");

            while (true) {

                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, byte[]> rec : records) {
                    GenericRecord r = AvroUtils.deserializeOrder(rec.value());

                    System.out.printf(
                            "DLQ Received → key=%s orderId=%s product=%s price=%.2f\n",
                            rec.key(),
                            r.get("orderId"),
                            r.get("product"),
                            r.get("price")
                    );
                }
            }
        }
    }
}
