package com.wentry.wmq.domain.listener;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.domain.TempBrokerState;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.topic.TopicInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.model.CompareResult;
import com.wentry.wmq.utils.MixUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
public class BrokerRegistryListener implements PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(BrokerRegistryListener.class);

    public BrokerRegistryListener(BrokerState brokerState, ZkRegistry registry) {
        this.brokerState = brokerState;
        this.registry = registry;
    }

    private final BrokerState brokerState;
    private final ZkRegistry registry;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

        String path = ZkPaths.getBrokerRegistryPath(brokerState.getClusterName());

        log.info("BrokerRegistryListener.childEvent(" + "client = " + client + ", event = " + event + ")");

        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            log.warn("stat null for path:{}", path);
            return;
        }
        int version = stat.getVersion();

        //本地版本号大于监听的节点，不做任何处理
        if (brokerState.getLastVersion() > version) {
            log.info("latestVersion :{} , gt node version:{} ", brokerState.getLastVersion(), version);
            return;
        }

        //根据新的broker列表合集，计算新的partitions，然后尝试更新partition的zk节点
        TempBrokerState newState = calculateBrokerState(
                brokerState, registry.getBrokers(brokerState.getClusterName())
        );

        //先更新broker列表
        updateBrokerList(brokerState, version, newState);

        if (!newState.isNeedNotify()) {
            log.info("calculateBrokerState res:{}", newState);
            return;
        }

        //比较当前版本
        int latestVersion = registry.getPathValue(ZkPaths.getPartitionRegistryVersionPath(brokerState.getClusterName()), Integer.class, 0);
        if (latestVersion > version) {
            log.info("latestVersion :{} , gt node version:{} ", brokerState.getLastVersion(), version);
            return;
        }

        //更新zk节点
        loopUpdatePartitions(brokerState, newState.getPartitions());

        //更新版本号
        registry.updateVal(CreateMode.PERSISTENT, ZkPaths.getPartitionRegistryVersionPath(brokerState.getClusterName()), version);

        //更新本地的partitions映射
        //这里通过broadcast触发
//                brokerState.updateLocalPartitions(newState.getPartitions());

        //周知broker\producer\consumer，partition变动
        brokerState.broadcastPartitionChange(newState.getPartitions());

    }

    public void loopUpdatePartitions(BrokerState brokerState, List<PartitionInfo> partitions) {
        for (PartitionInfo partition : partitions) {
            registry.updateVal(
                    CreateMode.PERSISTENT,
                    ZkPaths.getPartitionRegistryChildPath(brokerState.getClusterName(), partition.getTopic()),
                    partition
            );
        }
    }

    /**
     * 这里设计到分区的重新分配，一个broker的加入会带来一系列的连锁反应
     * 1。 会影响到partition的重新分配
     * 2。 会影响到brokerState中的主从节点的分布
     * 3。 会影响consumer中维护的partition映射
     * 4。 应该严格避免partition完全迁移到一个新的broker，因为消费的offset会失真，即新的broker本来是另外分区的offset，这样会错乱，问题很大！
     * 在kafka中，分区数和broker的数量都只能加，不能减，可能也就是这个原因，减少partition和broker，会很大程度上影响消息的
     * 5。 如果leader掉了，只能尝试将follower提升为leader，尽量保证消息的同步不丢
     * 6。 应该禁止频繁写入zk，因此和消息写入有关的记录，最好记录在broker客户端，比如ISR
     * 7。 ISR是保证主从消息同步的机制，如果ISR为空，则需要比对所有follower消息的版本号，取最新的follower提升为leader
     * 8。 kafka的ISR是存在控制器中的（一个特殊的Leader），
     */
    private TempBrokerState calculateBrokerState(BrokerState brokerState, List<BrokerInfo> registryBrokers) {

        Map<String, BrokerInfo> brokerInfoMap = registryBrokers.stream().collect(Collectors.toMap(BrokerInfo::getBrokerId, x -> x));

        List<PartitionInfo> partitions = registry.getPathChildrenValues(
                ZkPaths.getPartitionRegistryPath(brokerState.getClusterName()), PartitionInfo.class
        );

        //先移除掉，所有没有registry，但又在partition中的broker
        clearUnRegistryBrokerFromPartitions(partitions, registryBrokers);

        CompareResult compareResult = compareBrokers(brokerState.getBrokerInfoMap(), registryBrokers);
        log.info("compareResult:{}", JsonUtils.toJson(compareResult));
        if (CollectionUtils.isEmpty(compareResult.getLackBrokers())
                && CollectionUtils.isEmpty(compareResult.getNewBrokers())) {
            log.info("none diff found...");
            return new TempBrokerState().setMsg("none diff found").setBrokerInfoMap(brokerInfoMap);
        }

        //首先检查一下leader和follower是否有重合，如果有，则需要清理一遍
        for (PartitionInfo partition : partitions) {
            Map<Integer, String> partitionLeader = partition.getPartitionLeader();
            Map<Integer, Set<String>> partitionFollower = partition.getPartitionFollowers();
            for (Map.Entry<Integer, String> partitionLeaderEntry : partitionLeader.entrySet()) {
                Set<String> followers = partitionFollower.get(partitionLeaderEntry.getKey());
                if (CollectionUtils.isNotEmpty(followers)) {
                    followers.remove(partitionLeaderEntry.getValue());
                }
            }
        }

        processPartitionAllocate(partitions, compareResult);

//        /**
//         * 若有缺失的，则进行主从切换
//         * 1. 缺失的为主，则需要主从切换，
//         *         若无Follower，则忽略，此时整个partition将会不再有新的写入，在推送到consumer之后，也不会有消费了
//         * 2. 缺失的是从，直接更新信息即可
//         */
//        if (CollectionUtils.isNotEmpty(compareResult.getLackBrokers())) {
//            processLackBrokers(partitions, compareResult);
//        }
//
//        /**
//         * 若有新增的，则将新增的节点，放入到follower集合中
//         * 注意：
//         * 1。 不能将主节点转移，而是直接将新加入的主节点放到follower集合中
//         * 2。 新加入的节点什么时候成为主节点？ 有新增topic的时候，或者新增partition的时候
//         * 3。 总之一个原则，不能影响offset的参照物！！！
//         * 4。 也就是说，新加入的节点在刚开始肯定不是Leader
//         */
//        if (CollectionUtils.isNotEmpty(compareResult.getNewBrokers())) {
//        }

        //replica re-check
        ensureFollowerCount(partitions, registryBrokers);

        //这里统计完了之后，返回快照
        return new TempBrokerState().setPartitions(partitions).setBrokerInfoMap(brokerInfoMap).setNeedNotify(true);
    }

    private void clearUnRegistryBrokerFromPartitions(List<PartitionInfo> partitions, List<BrokerInfo> registryBrokers) {
        Set<String> brokerIdSet = registryBrokers.stream().map(BrokerInfo::getBrokerId).collect(Collectors.toSet());
        for (PartitionInfo partition : partitions) {
            partition.getPartitionLeader().entrySet().removeIf(
                    next -> !brokerIdSet.contains(next.getValue())
            );

            for (Map.Entry<Integer, Set<String>> ety : partition.getPartitionFollowers().entrySet()) {
                ety.getValue().removeIf(s -> !brokerIdSet.contains(s));
            }
        }
    }

    private void ensureFollowerCount(List<PartitionInfo> partitions, List<BrokerInfo> registryBrokers) {

        Map<String, Integer> idIdx = new ConcurrentHashMap<>();
        for (int i = 0; i < registryBrokers.size(); i++) {
            idIdx.put(registryBrokers.get(i).getBrokerId(), i);
        }

        for (PartitionInfo partition : partitions) {
            TopicInfo topicInfo = registry.getPathValue(ZkPaths.getTopicRegistryNode(brokerState.getClusterName(), partition.getTopic()), TopicInfo.class);
            if (topicInfo == null) {
                continue;
            }
            for (Map.Entry<Integer, Set<String>> partitionFollowers : partition.getPartitionFollowers().entrySet()) {
                Set<String> followers = partitionFollowers.getValue();
                List<Integer> exclude = new ArrayList<>();
                while (followers.size() < topicInfo.getReplica()) {
                    //尝试补齐follower
                    String leaderId = partition.getPartitionLeader().get(partitionFollowers.getKey());
                    exclude.add(idIdx.get(leaderId));
                    Integer follower = MixUtils.randomGetIdx(registryBrokers, exclude);
                    if (follower==null) {
                        break;
                    }
                    followers.add(registryBrokers.get(follower).getBrokerId());
                    exclude.add(follower);
                }
                //noinspection LoopStatementThatDoesntLoop
                while (!followers.isEmpty() && followers.size() > topicInfo.getReplica()) {
                    MixUtils.randomRm(followers);
                    break;
                }
            }
        }
    }

    private void processLackBrokers(List<PartitionInfo> partitions, CompareResult compareResult) {
        List<BrokerInfo> lackBrokers = compareResult.getLackBrokers();
        Map<String, BrokerInfo> lackBrokersMap = lackBrokers.stream().collect(
                Collectors.toMap(BrokerInfo::getBrokerId, x -> x, (o, n) -> n)
        );
        for (PartitionInfo partition : partitions) {
            //遍历主节点，如果有缺失的，进行主从切换
            for (Map.Entry<Integer, String> partitionLeaders : partition.getPartitionLeader().entrySet()) {
                Integer partitionIdx = partitionLeaders.getKey();
                String leaderBrokerId = partitionLeaders.getValue();
                if (lackBrokersMap.get(leaderBrokerId) != null) {
                    //leader缺失了，进行主从切换
                    Map<Integer, Set<String>> followers = partition.getPartitionFollowers();
                    if (MapUtils.isEmpty(followers) || CollectionUtils.isEmpty(followers.get(partitionIdx))) {
                        //leader下线的followers也是空的
                        log.info("partition leader and follower both null, partition:{}", JsonUtils.toJson(partition));
                    } else {
                        Set<String> fls = followers.get(partitionIdx);
                        //这里选取，可以引入ISR机制
                        String pop = MixUtils.randomRm(fls);
                        //变为主节点
                        partition.getPartitionLeader().put(partitionIdx, pop);
                        partition.getPartitionFollowers().get(partitionIdx).remove(pop);
                    }
                }
            }

            //遍历从节点，如果有缺失的，直接移除
            for (Map.Entry<Integer, Set<String>> ety : partition.getPartitionFollowers().entrySet()) {
                for (BrokerInfo lackBroker : lackBrokers) {
                    ety.getValue().remove(lackBroker.getBrokerId());
                }
            }
        }

        log.info("BrokerRegistryListener.processLackBrokers(" + "partitions = " + JsonUtils.toJson(partitions) + ", compareResult = " + JsonUtils.toJson(compareResult) + ")");
    }

    private void processPartitionAllocate(List<PartitionInfo> partitions, CompareResult compareResult) {
        log.info("BrokerRegistryListener.processPartitionAllocate before(" + "partitions = "
                + JsonUtils.toJson(partitions) + ", compareResult = " + JsonUtils.toJson(compareResult) + ")");
        for (PartitionInfo partition : partitions) {
            processLeaderAllocate(compareResult, partition);
            processFollowerAllocate(compareResult, partition);
        }
        log.info("BrokerRegistryListener.processPartitionAllocate after(" + "partitions = "
                + JsonUtils.toJson(partitions) + ", compareResult = " + JsonUtils.toJson(compareResult) + ")");
    }

    private boolean currLeaderNotRegistry(PartitionInfo partition) {
        return false;
    }

    private void processFollowerAllocate(CompareResult compareResult, PartitionInfo partition) {
        TopicInfo topicInfo = registry.getPathValue(ZkPaths.getTopicRegistryNode(
                brokerState.getClusterName(), partition.getTopic()), TopicInfo.class
        );
        if (topicInfo == null) {
            return;
        }
        int addCount = calcAverFollowerPartitions(partition);
        Map<Integer, Set<String>> partitionFollowers = partition.getPartitionFollowers();
        List<Integer> leastFollowerPartitionList = new ArrayList<>(partitionFollowers.keySet());
        //按follower个数排序，从小到大排序
        leastFollowerPartitionList.sort(Comparator.comparingInt(o -> partitionFollowers.get(o).size()));
        for (int i = 0; i < addCount; i++) {
            for (BrokerInfo newBroker : compareResult.getNewBrokers()) {
                //加入到follower中
                Set<String> followers = partitionFollowers.computeIfAbsent(
                        getPartition(leastFollowerPartitionList, i),
                        integer -> new HashSet<>()
                );

                if (followers.size() >= topicInfo.getReplica()) {
                    continue;
                }

                Integer p = getPartition(leastFollowerPartitionList, i);
                //brokerId已经是当前partition的leader，则直接跳过
                String leaderId = partition.getPartitionLeader().get(p);
                if (StringUtils.equals(leaderId, newBroker.getBrokerId())) {
                    continue;
                }
            }
        }
    }

    private Integer getPartition(List<Integer> followedPartitionIdx, int num) {
        if (followedPartitionIdx.size() > num) {
            return followedPartitionIdx.get(num);
        }
        return num;
    }

    private void processLeaderAllocate(CompareResult compareResult, PartitionInfo partition) {
        List<BrokerInfo> allBrokers = compareResult.getAllBrokers();
        Map<String, Integer> idIdx = new ConcurrentHashMap<>();
        for (int i = 0; i < allBrokers.size(); i++) {
            idIdx.put(allBrokers.get(i).getBrokerId(), i);
        }

        //直接拉取topicInfo
        TopicInfo topicInfo = getTopicInfo(brokerState.getClusterName(), partition.getTopic());
        if (topicInfo == null) {
            return;
        }
        for (int pIdx = 0; pIdx < topicInfo.getPartition(); pIdx++) {

            //有leader了，跳过
            if (partition.getPartitionLeader().containsKey(pIdx)) {
                continue;
            }

            //选取主节点
            //从备份中选
            Integer leaderIdx;
            if (CollectionUtils.isNotEmpty(partition.getPartitionFollowers().get(pIdx))) {
                //这里可以加入ISR机制
                String leaderId = MixUtils.randomRm(partition.getPartitionFollowers().get(pIdx));
                leaderIdx = idIdx.get(leaderId);
            }else{
                leaderIdx = MixUtils.randomGetIdx(allBrokers, null);
            }
            if (leaderIdx == null) {
                log.warn("leaderIdx null ..., newBrokers:{}", JsonUtils.toJson(allBrokers));
                continue;
            }
            partition.getPartitionLeader().put(pIdx, allBrokers.get(leaderIdx).getBrokerId());
            partition.getPartitionFollowers().computeIfAbsent(pIdx, integer -> new HashSet<>())
                    .remove(allBrokers.get(leaderIdx).getBrokerId());
            //每个partition，选取从节点
            List<Integer> excludes = new ArrayList<>();
            excludes.add(leaderIdx);
            //设计replica节点，需考虑replica大于当前节点数
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

    private TopicInfo getTopicInfo(String clusterName, String topic) {
        return registry.getPathValue(ZkPaths.getTopicRegistryNode(clusterName, topic), TopicInfo.class);
    }


    //简单计算一下负载情况
    private int calcAverFollowerPartitions(PartitionInfo partition) {
        Set<String> ss = new HashSet<>();
        for (Map.Entry<Integer, Set<String>> ety : partition.getPartitionFollowers().entrySet()) {
            ss.addAll(ety.getValue());
        }
        //分区数/当前的follower数 = 每个follower当前平均平均负载
        return Math.max(1, partition.getPartitionFollowers().keySet().size() / (Math.max(1, ss.size())));
    }

    private CompareResult compareBrokers(Map<String, BrokerInfo> localBrokerMap, List<BrokerInfo> registryBrokers) {

        CompareResult res = new CompareResult();

        List<BrokerInfo> newBrokers = new ArrayList<>();
        List<BrokerInfo> lackBrokers = new ArrayList<>();
        Collection<BrokerInfo> values = localBrokerMap.values();
        for (BrokerInfo broker : registryBrokers) {
            if (!values.contains(broker)) {
                //老的无，则为新增
                newBrokers.add(broker);
            }
        }

        for (BrokerInfo value : values) {
            if (!registryBrokers.contains(value)) {
                //缺失的
                lackBrokers.add(value);
            }
        }

        return res.setLackBrokers(lackBrokers).setNewBrokers(newBrokers).setAllBrokers(registryBrokers);
    }

    private void updateBrokerList(BrokerState brokerState, int version, TempBrokerState newState) {
        brokerState.setBrokerInfoMap(newState.getBrokerInfoMap());
        brokerState.setLastVersion(version);
    }

}
