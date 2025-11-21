package com.assignment.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtils {

    private static final String SCHEMA_JSON =
            "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Order\",\n" +
            "  \"namespace\": \"com.assignment.avro\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"orderId\", \"type\": \"string\" },\n" +
            "    { \"name\": \"product\", \"type\": \"string\" },\n" +
            "    { \"name\": \"price\", \"type\": \"float\" }\n" +
            "  ]\n" +
            "}";

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public static byte[] serializeOrder(String orderId, String product, float price) throws IOException {

        GenericRecord order = new GenericData.Record(SCHEMA);
        order.put("orderId", orderId);
        order.put("product", product);
        order.put("price", price);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);

        writer.write(order, encoder);
        encoder.flush();

        return output.toByteArray();
    }

    public static GenericRecord deserializeOrder(byte[] data) {
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);

        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Avro Order record", e);
        }
    }
}
