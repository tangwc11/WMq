package com.wentry.wmq.domain.dashboard;

import com.wentry.wmq.domain.registry.partition.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Author: tangwc
 */
public class TopicDetail {

    private TopicBaseInfo baseInfo;
    private List<TopicBrokerInfo> brokers = new ArrayList<>();
    private Map<String, List<TopicConsumerInfo>> groupConsumers = new ConcurrentHashMap<>();

    public List<TopicBrokerInfo> getBrokers() {
        return brokers;
    }

    public TopicDetail setBrokers(List<TopicBrokerInfo> brokers) {
        this.brokers = brokers;
        return this;
    }

    public TopicBaseInfo getBaseInfo() {
        return baseInfo;
    }

    public TopicDetail setBaseInfo(TopicBaseInfo baseInfo) {
        this.baseInfo = baseInfo;
        return this;
    }

    public Map<String, List<TopicConsumerInfo>> getGroupConsumers() {
        return groupConsumers;
    }

    public TopicDetail setGroupConsumers(Map<String, List<TopicConsumerInfo>> groupConsumers) {
        this.groupConsumers = groupConsumers;
        return this;
    }

}
