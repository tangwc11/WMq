package com.wentry.wmq.domain.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * @Description:
 * @Author: tangwc
 */
public class WMqMessage implements Serializable {
    private static final long serialVersionUID = -6142483360347991518L;

    /**
     * 消息唯一ID
     */
    private String msgId = UUID.randomUUID().toString();

    /**
     * topic
     */
    private String topic;

    /**
     * 内容
     */
    private String msg;

    /**
     * 可以划分为固定的partition
     */
    private String key;

    /**
     * 划分到的分区
     */
    private int partition;

    /**
     * 为了实现唯一消息
     */
    private String uniq;

    private long createTime = System.currentTimeMillis();

    public long getCreateTime() {
        return createTime;
    }

    public WMqMessage setCreateTime(long createTime) {
        this.createTime = createTime;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public WMqMessage setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public WMqMessage setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public WMqMessage setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public String getKey() {
        return key;
    }

    public WMqMessage setKey(String key) {
        this.key = key;
        return this;
    }

    public String getUniq() {
        return uniq;
    }

    public WMqMessage setUniq(String uniq) {
        this.uniq = uniq;
        return this;
    }

    public String getMsgId() {
        return msgId;
    }

    public WMqMessage setMsgId(String msgId) {
        this.msgId = msgId;
        return this;
    }

    @Override
    public String toString() {
        return "WMqMessage{" +
                "topic='" + topic + '\'' +
                ", msg='" + msg + '\'' +
                ", key='" + key + '\'' +
                ", partition=" + partition +
                ", uniq='" + uniq + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
