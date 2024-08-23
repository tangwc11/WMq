package com.wentry.wmq.domain.listener;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.domain.registry.topic.TopicInfo;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.utils.MixUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TopicRegistryListener implements NodeCacheListener {

    private static final Logger log = LoggerFactory.getLogger(TopicRegistryListener.class);

    private final ZkRegistry registry;
    private final BrokerState brokerState;

    public TopicRegistryListener(BrokerState brokerState, ZkRegistry registry) {
        this.registry = registry;
        this.brokerState = brokerState;
    }

    @Override
    public void nodeChanged() throws Exception {

        log.info("TopicRegistryListener triggered...");

        List<BrokerInfo> allBrokers = registry.getBrokers(brokerState.getClusterName());
        if (CollectionUtils.isEmpty(allBrokers)) {
            return;
        }

        List<PartitionInfo> partitions = registry.getPathChildrenValues(
                ZkPaths.getPartitionRegistryPath(brokerState.getClusterName()), PartitionInfo.class
        );

        List<PartitionInfo> changedPartition = new ArrayList<>();

        for (PartitionInfo partition : partitions) {

            //有空的partition，重新分配
            if (MapUtils.isEmpty(partition.getPartitionLeader())) {

                changedPartition.add(partition);

                //直接拉取topicInfo
                TopicInfo topicInfo = registry.getPathValue(
                        ZkPaths.getTopicRegistryNode(brokerState.getClusterName(), partition.getTopic()),
                        TopicInfo.class
                );
                if (topicInfo == null) {
                    continue;
                }
                for (int pIdx = 0; pIdx < topicInfo.getPartition(); pIdx++) {
                    //选取主节点
                    Integer leaderIdx = MixUtils.randomGetIdx(allBrokers, null);
                    if (leaderIdx == null) {
                        log.warn("leaderIdx null ..., newBrokers:{}", JsonUtils.toJson(allBrokers));
                        continue;
                    }
                    partition.getPartitionLeader().put(pIdx, allBrokers.get(leaderIdx).getBrokerId());
                    //每个partition，选取从节点
                    List<Integer> excludes = new ArrayList<>();
                    excludes.add(leaderIdx);
                    for (int j = 0; j < topicInfo.getReplica(); j++) {
                        Integer newFollowerIdx = MixUtils.randomGetIdx(allBrokers, excludes);
                        if (newFollowerIdx == null) {
                            //没有follower剩余
                            break;
                        }
                        partition.getPartitionFollowers()
                                .computeIfAbsent(pIdx, integer -> new HashSet<>())
                                .add(allBrokers.get(newFollowerIdx).getBrokerId());
                        excludes.add(newFollowerIdx);
                    }
                }
            }
        }


//        brokerState.updateLocalPartitions(partitions);

        for (PartitionInfo partition : changedPartition) {
            registry.updateVal(
                    CreateMode.PERSISTENT,
                    ZkPaths.getPartitionRegistryChildPath(brokerState.getClusterName(), partition.getTopic()),
                    partition
            );
        }


        brokerState.broadcastPartitionChange(partitions);
    }



}

