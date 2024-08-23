package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReplicaSyncPushReq implements Serializable {
    private static final long serialVersionUID = -7509370059296731025L;

    private String topic;
    private int partition;
    private byte[] bytes;
    private long preOffset;

    public String getTopic() {
        return topic;
    }

    public ReplicaSyncPushReq setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public ReplicaSyncPushReq setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public ReplicaSyncPushReq setBytes(byte[] bytes) {
        this.bytes = bytes;
        return this;
    }

    public long getPreOffset() {
        return preOffset;
    }

    public ReplicaSyncPushReq setPreOffset(long preOffset) {
        this.preOffset = preOffset;
        return this;
    }
}
