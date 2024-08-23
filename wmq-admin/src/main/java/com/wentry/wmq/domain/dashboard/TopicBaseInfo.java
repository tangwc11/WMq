package com.wentry.wmq.domain.dashboard;

/**
 * @Description:
 * @Author: tangwc
 */
public class TopicBaseInfo {

    String name;
    int partition;
    int replica;

    public String getName() {
        return name;
    }

    public TopicBaseInfo setName(String name) {
        this.name = name;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public TopicBaseInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public int getReplica() {
        return replica;
    }

    public TopicBaseInfo setReplica(int replica) {
        this.replica = replica;
        return this;
    }
}
