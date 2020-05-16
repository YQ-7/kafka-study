package com.study.kafka.consumer;

import com.study.kafka.producer.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {

    private static final String CHARSET = "UTF-8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, CHARSET);
            address = new String(addressBytes, CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return Company.builder()
                .name(name)
                .address(address)
                .build();
    }

    @Override
    public void close() {

    }
}
