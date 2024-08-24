package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReplicaSyncPullReq implements Serializable {
    private static final long serialVersionUID = 3433035889170723627L;

    private String topic;
    private int partition;
    private long offset;
    private int pullSize;
    private String pullBrokerGroup;
    private String from;

    public String getFrom() {
        return from;
    }

    public ReplicaSyncPullReq setFrom(String from) {
        this.from = from;
        return this;
    }

    public String getPullBrokerGroup() {
        return pullBrokerGroup;
    }

    public ReplicaSyncPullReq setPullBrokerGroup(String pullBrokerGroup) {
        this.pullBrokerGroup = pullBrokerGroup;
        return this;
    }

    public int getPullSize() {
        return pullSize;
    }

    public ReplicaSyncPullReq setPullSize(int pullSize) {
        this.pullSize = pullSize;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public ReplicaSyncPullReq setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public ReplicaSyncPullReq setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public ReplicaSyncPullReq setOffset(long offset) {
        this.offset = offset;
        return this;
    }
}
