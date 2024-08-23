package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class PullMsgReq implements Serializable {
    private static final long serialVersionUID = 5729715172068125805L;

    private String topic;
    private int partition;
    private long startOffset;
    private int pullSize;
    private String group;

    public String getGroup() {
        return group;
    }

    public PullMsgReq setGroup(String group) {
        this.group = group;
        return this;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public PullMsgReq setStartOffset(long startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public PullMsgReq setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public PullMsgReq setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public int getPullSize() {
        return pullSize;
    }

    public PullMsgReq setPullSize(int pullSize) {
        this.pullSize = pullSize;
        return this;
    }
}
