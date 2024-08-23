package com.wentry.wmq.domain.dashboard;

import java.util.Set;

/**
 * @Description:
 * @Author: tangwc
 */
public class TopicBrokerInfo {

    int partition;
    String leader;
    Set<String> followers;

    public int getPartition() {
        return partition;
    }

    public TopicBrokerInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getLeader() {
        return leader;
    }

    public TopicBrokerInfo setLeader(String leader) {
        this.leader = leader;
        return this;
    }

    public Set<String> getFollowers() {
        return followers;
    }

    public TopicBrokerInfo setFollowers(Set<String> followers) {
        this.followers = followers;
        return this;
    }
}
