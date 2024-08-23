package com.wentry.wmq.domain.registry.partition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Author: tangwc
 *
 * 分区节点，节点名为topic
 */
public class PartitionInfo {

    private String topic;
    private int partitionTotal;
    private Map<Integer, String> partitionLeader = new ConcurrentHashMap<>();
    private Map<Integer, Set<String>> partitionFollowers = new ConcurrentHashMap<>();

    public PartitionInfo() {
    }

    public String getTopic() {
        return topic;
    }

    public PartitionInfo setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartitionTotal() {
        return partitionTotal;
    }

    public PartitionInfo setPartitionTotal(int partitionTotal) {
        this.partitionTotal = partitionTotal;
        return this;
    }

    public Map<Integer, String> getPartitionLeader() {
        return partitionLeader;
    }

    public PartitionInfo setPartitionLeader(Map<Integer, String> partitionLeader) {
        this.partitionLeader = partitionLeader;
        return this;
    }

    public Map<Integer, Set<String>> getPartitionFollowers() {
        return partitionFollowers;
    }

    public PartitionInfo setPartitionFollowers(Map<Integer, Set<String>> partitionFollowers) {
        this.partitionFollowers = partitionFollowers;
        return this;
    }

}
