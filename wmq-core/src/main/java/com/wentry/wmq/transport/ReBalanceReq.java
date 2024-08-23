package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReBalanceReq implements Serializable {
    private static final long serialVersionUID = -6336114640476111651L;

    String topic;
    String group;
    int partition;

    public String getTopic() {
        return topic;
    }

    public ReBalanceReq setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getGroup() {
        return group;
    }

    public ReBalanceReq setGroup(String group) {
        this.group = group;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public ReBalanceReq setPartition(int partition) {
        this.partition = partition;
        return this;
    }
}
