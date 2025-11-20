package com.assignment.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.*;

public class AvroUtils {

    private static final Schema ORDER_SCHEMA;

    static {
        try (InputStream in = AvroUtils.class.getClassLoader().getResourceAsStream("avro/order.avsc")) {
            if (in == null)
                throw new IllegalStateException("Cannot find avro/order.avsc");
            ORDER_SCHEMA = new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load schema", e);
        }
    }

    public static byte[] serializeOrder(String orderId, String product, float price) {
        GenericRecord record = new GenericData.Record(ORDER_SCHEMA);
        record.put("orderId", orderId);
        record.put("product", product);
        record.put("price", price);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(ORDER_SCHEMA).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static GenericRecord deserializeOrder(byte[] data) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
            return new GenericDatumReader<GenericRecord>(ORDER_SCHEMA).read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
