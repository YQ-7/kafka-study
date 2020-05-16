package com.study.kafka.adminclient;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicOperation {

    private static final String brokerList = "server01:9092,server02:9092,server03:9092";
    private static final String topic = "topic-demo-admin";

    public static void createTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        // 方式一
        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);


        // 方式二
//        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
//        replicasAssignments.put(0, Arrays.asList(0));
//        replicasAssignments.put(1, Arrays.asList(0));
//        replicasAssignments.put(2, Arrays.asList(0));
//        replicasAssignments.put(3, Arrays.asList(0));
//
//        NewTopic newTopic = new NewTopic(topic, replicasAssignments);

        // 修改配置
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        newTopic.configs(configs);

        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void describeTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> descriptionMap = result.all().get();
            System.out.println(descriptionMap);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void updateTopicPartitions() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        try {
             result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void deleteTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        DeleteTopicsResult result = client.deleteTopics(Collections.singleton(topic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void main(String[] args) {
        createTopic();
        describeTopic();
        updateTopicPartitions();
        describeTopic();
        deleteTopic();
    }
}
