package com.study.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 发送Company示例，使用自定义的序列化器
 */
public class ProducerCompany {

    public static final String brokerList = "server01:9092,server02:9092,server03:9092";

    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        // 设置bootstrap.servers：kafka集群broker列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 设置key.serializer：key的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置value.serializer：value的序列化器
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        // 使用Protostuff序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanyProtostuffSerializer.class.getName());
        // 设置client.id：消费端id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return props;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Company> producer = new KafkaProducer<String, Company>(initConfig());
        Company company = Company.builder()
                .name("hidenkafka2")
                .address("China")
                .build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        producer.send(record).get();
    }
}
