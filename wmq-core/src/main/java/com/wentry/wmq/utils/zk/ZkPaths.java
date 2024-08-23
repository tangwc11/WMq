package com.wentry.wmq.utils.zk;

/**
 * @Description:
 * @Author: tangwc
 */
public class ZkPaths {

    public static final String BASE = "/wmq";

    public static final String TOPIC_REGISTRY = "topic-registry";
    public static final String TOPIC_REGISTRY_SIGNAL = "topic-registry-signal";

    public static final String BROKER_REGISTRY = "broker-registry";
    public static final String PARTITION_REGISTRY = "partition-registry";
    public static final String PARTITION_REGISTRY_VERSION = "partition-registry-version";

    public static final String CLIENT_REGISTRY = "client-registry";//永久目录

    public static final String CONSUMER_OFFSET_DIR = "consumer-offset";//永久目录
    public static final String CONSUMER_OFFSET_NODE = "[%s|%s]";//${group}-${partition}";//消费进度 永久节点

    public static final String CONSUMER_GROUP = "consumer-group-registry";//消费组关系，永久节点
    public static final String CONSUMER_GROUP_INST_NODE = "[%s|%s|%s]";//${topic}-${group}-${partition}";//临时节点


    public static String join(String... nodes) {
        return String.join("/", nodes);
    }

    public static String getPartitionRegistryVersionPath(String clusterName) {
        return join(BASE, clusterName, PARTITION_REGISTRY_VERSION);
    }

    public static String getPartitionRegistryPath(String clusterName) {
        return join(BASE, clusterName, PARTITION_REGISTRY);
    }

    public static String getPartitionRegistryChildPath(String clusterName, String topic) {
        return join(BASE, clusterName, PARTITION_REGISTRY, topic);
    }

    public static String getClientRegistryPathNode(String clusterName,String clientId) {
        return join(BASE, clusterName, CLIENT_REGISTRY, clientId);
    }

    public static String getClientRegistryPath(String clusterName) {
        return join(BASE, clusterName, CLIENT_REGISTRY);
    }

    public static String getBrokerRegistryPath(String clusterName) {
        return join(BASE, clusterName, BROKER_REGISTRY);
    }

    public static String getBrokerRegistryNodePath(String clusterName, String brokerId) {
        return join(BASE, clusterName, BROKER_REGISTRY, brokerId);
    }

    public static String getTopicRegistrySignal(String clusterName) {
        return join(BASE, clusterName, TOPIC_REGISTRY_SIGNAL);
    }

    public static String getTopicRegistryDir(String clusterName) {
        return join(BASE, clusterName, TOPIC_REGISTRY);
    }
    public static String getTopicRegistryNode(String clusterName, String topic) {
        return join(BASE, clusterName, TOPIC_REGISTRY, topic);
    }

    public static String getConsumerInstanceNode(String clusterName, String topic, String consumerGroup, Integer partition) {
        return join(BASE, clusterName, CONSUMER_GROUP, String.format(CONSUMER_GROUP_INST_NODE, topic, consumerGroup, partition));
    }

    public static String getConsumerOffsetDir(String clusterName, String topic) {
        return join(BASE, clusterName, CONSUMER_OFFSET_DIR, topic);
    }

    public static String getConsumerOffset(String clusterName, String topic, String consumerGroup, int partition) {
        return join(BASE, clusterName, CONSUMER_OFFSET_DIR, topic, String.format(CONSUMER_OFFSET_NODE, consumerGroup, partition));
    }
}
