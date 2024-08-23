package com.wentry.wmq.domain.dashboard;

/**
 * @Description:
 * @Author: tangwc
 */
public class TopicConsumerInfo {

    String msg;
    String group;
    int partition;
    Long offset;
    String consumerInstance;

    public String getMsg() {
        return msg;
    }

    public TopicConsumerInfo setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public String getGroup() {
        return group;
    }

    public TopicConsumerInfo setGroup(String group) {
        this.group = group;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public TopicConsumerInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public Long getOffset() {
        return offset;
    }

    public TopicConsumerInfo setOffset(Long offset) {
        this.offset = offset;
        return this;
    }

    public String getConsumerInstance() {
        return consumerInstance;
    }

    public TopicConsumerInfo setConsumerInstance(String consumerInstance) {
        this.consumerInstance = consumerInstance;
        return this;
    }
}
