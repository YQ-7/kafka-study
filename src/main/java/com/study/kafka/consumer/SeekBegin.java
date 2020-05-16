package com.study.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 使用seek从分区开始位置消费(开始位置会随数据过期自然增加)
 */
public class SeekBegin {
    public static final String brokerList = "server01:9092,server02:9092,server03:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();

        // 若assignment非空，则已成功分配到了分区
        while (assignment.isEmpty()) {
            // poll中会执行消费者分区分配
            consumer.poll(Duration.ofMillis(100));
            // 获取当前消费者分配到的分区
            assignment = consumer.assignment();
        }
        System.out.println(assignment);

        // 方式一
        // 获取分配到的分区的开始位置
        Map<TopicPartition, Long> offsets = consumer.beginningOffsets(assignment);
        // 通过seek指定分区消费位移位置
        for (TopicPartition tp: assignment) {
            consumer.seek(tp, offsets.get(tp));
        }

        //方式二
//        consumer.seekToBeginning(assignment);
        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            // consumer the record
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(
                        record.topic() + ":"
                        + record.partition() + ":"
                        + record.offset() + ":"
                        + record.value());
            }
        }

    }

}
