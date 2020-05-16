package com.study.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  处理消息模块改成多线程
 *  消费者拉取分批次的消息，提交给多线程进行处理
 *  共享变量offsets用来记录需提交的位移，读写需要加锁
 */
public class ThirdMultiConsumerThreadDemo {
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
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                props,
                topic,
                Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        public KafkaConsumerThread(Properties props, String topic, int threadNumber) {
            this.kafkaConsumer = new KafkaConsumer<String, String>(props);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber,
                    0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (isRunning.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records, offsets));
                    }
                    // 提交分区位移
                    synchronized (offsets) {
                        if (!offsets.isEmpty()) {
                            kafkaConsumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                this.kafkaConsumer.close();
            }
        }
    }

    public static class RecordsHandler extends Thread {
        public final ConsumerRecords<String, String> records;
        private Map<TopicPartition, OffsetAndMetadata> offsets;

        public RecordsHandler(ConsumerRecords<String, String> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.records = records;
            this.offsets = offsets;
        }

        @Override
        public void run() {
            for (TopicPartition tp: records.partitions()) {
                // 处理分区消息
                List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
                for (ConsumerRecord<String, String> record : tpRecords) {
                    System.out.println(
                            record.topic() + ":"
                                    + record.partition() + ":"
                                    + record.offset() + ":"
                                    + record.value());
                }
                // 更新分区消费位移
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                synchronized (offsets) {
                    if (!offsets.containsKey(tp)) {
                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                    } else {
                        long position = offsets.get(tp).offset();
                        if (position < lastConsumedOffset + 1) {
                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }
                    }
                }
            }
        }
    }
}
