package com.wentry.wmq.domain.registry.offset;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class OffsetInfo implements Serializable {

    private static final long serialVersionUID = 5436649231775093990L;

    private String group;
    private String topic;
    private int partition;
    private long offset;

    public String getGroup() {
        return group;
    }

    public OffsetInfo setGroup(String group) {
        this.group = group;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public OffsetInfo setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public OffsetInfo setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public OffsetInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }
}
