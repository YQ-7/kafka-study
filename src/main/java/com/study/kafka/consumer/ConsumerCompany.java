package com.study.kafka.consumer;

import com.study.kafka.producer.Company;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerCompany {

    public static final String brokerList = "server01:9092,server02:9092,server03:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                CompanyDeserializer.class.getName());

        // 使用Protostuff反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CompanyProtostuffDeserializer.class.getName());
        return props;
    }

    public static void main(String[] args) {
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(initConfig());
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Company> record : records) {
                    System.out.println("topic = " + record.topic()
                            + ", partition = " + record.partition()
                            + ", offset = " + record.offset());
                    System.out.println("key = " + record.key()
                            + ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
