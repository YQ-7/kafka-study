package com.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 使用seek从第一条消息≥指定时间戳的消息位置开始消费
 */
public class SeekByTimestamps {
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

        // 获取分区一天之前的最近消息位置
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition tp: assignment) {
            timestampToSearch.put(tp, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);

        // 通过seek指定分区消费位移位置
        for (TopicPartition tp: assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }

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
