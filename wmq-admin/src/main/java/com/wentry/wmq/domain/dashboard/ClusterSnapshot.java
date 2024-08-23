package com.wentry.wmq.domain.dashboard;

import com.wentry.wmq.domain.registry.topic.TopicInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Author: tangwc
 */
public class ClusterSnapshot implements Serializable {
    private static final long serialVersionUID = -3350807872171553916L;

    String clusterName;
    List<TopicInfo> topics = new ArrayList<>();
    Map<String, TopicDetail> topicDetails = new ConcurrentHashMap<>();

    public String getClusterName() {
        return clusterName;
    }

    public ClusterSnapshot setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public List<TopicInfo> getTopics() {
        return topics;
    }

    public ClusterSnapshot setTopics(List<TopicInfo> topics) {
        this.topics = topics;
        return this;
    }

    public Map<String, TopicDetail> getTopicDetails() {
        return topicDetails;
    }

    public ClusterSnapshot setTopicDetails(Map<String, TopicDetail> topicDetails) {
        this.topicDetails = topicDetails;
        return this;
    }
}
