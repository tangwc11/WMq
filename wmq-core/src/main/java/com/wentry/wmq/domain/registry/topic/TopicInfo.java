package com.wentry.wmq.domain.registry.topic;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class TopicInfo implements Serializable {
    private static final long serialVersionUID = -734287655251741245L;

    private String topic;
    private int partition;
    private int replica;

    public String getTopic() {
        return topic;
    }

    public TopicInfo setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public TopicInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public int getReplica() {
        return replica;
    }

    public TopicInfo setReplica(int replica) {
        this.replica = replica;
        return this;
    }

}
