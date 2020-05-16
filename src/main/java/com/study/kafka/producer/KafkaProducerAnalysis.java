package com.study.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 生产者示例
 */
public class KafkaProducerAnalysis {

    public static final String brokerList = "server01:9092,server02:9092,server03:9092";

    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        // 设置bootstrap.servers：kafka集群broker列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // 设置key.serializer：key的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置value.serializer：value的序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置client.id：消费端id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        // 设置retries：抛出可重试异常时的重试次数，默认值10
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "msg","hi, Kafka!");
        // 发送方式一：发后即忘(fire-and-forget)
        producer.send(record);
//        try {
//            // 发送方式二：同步(sync)
//            Future<RecordMetadata> metadataFuture = producer.send(record);
//            RecordMetadata metadata = metadataFuture.get();
//            System.out.println(metadata.topic() + "-"
//                    + metadata.partition() + ":"
//                    + metadata.offset());
//
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
        // 发送方式三：异步
//        // Callback：回调函数，保证分区有序
//        producer.send(record, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                if (null != e) {
//                    e.printStackTrace();
//                } else {
//                    System.out.println(recordMetadata.topic() + "-"
//                            + recordMetadata.partition() + ":"
//                            + recordMetadata.offset());
//                }
//            }
//        });
//
//
//        // 发送多条消息
//        int i = 0;
//        while (i < 5) {
//            record = new ProducerRecord<>(topic, "msg" + i++);
//            try {
//                producer.send(record).get();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        }
        // close方法回收资源
        producer.close();
    }
}
