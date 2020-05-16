package com.study.kafka.ttl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTTL {

    public static final String brokerList = "server01:9092,server02:9092,server03:9092";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(initConfig());
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                topic,
                null,
                System.currentTimeMillis(),
                null,
                "msg_ttl_1",
                new RecordHeaders()
                        .add(new RecordHeader("ttl", BytesUtils.longToBytes(20)))
        );

        ProducerRecord<String, String> record2 = new ProducerRecord<>(
                topic,
                null,
                System.currentTimeMillis(),
                null,
                "msg_ttl_2",
                new RecordHeaders()
                        .add(new RecordHeader("ttl", BytesUtils.longToBytes(5)))
        );

        ProducerRecord<String, String> record3 = new ProducerRecord<>(
                topic,
                null,
                System.currentTimeMillis(),
                null,
                "msg_ttl_3",
                new RecordHeaders()
                        .add(new RecordHeader("ttl", BytesUtils.longToBytes(30)))
        );

        producer.send(record1);
        producer.send(record2);
        producer.send(record3);

        producer.close();
    }
}
