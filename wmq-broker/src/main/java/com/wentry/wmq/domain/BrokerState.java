package com.wentry.wmq.domain;

import com.wentry.wmq.domain.isr.IsrSet;
import com.wentry.wmq.domain.listener.TopicRegistryListener;
import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.transport.PartitionSyncResp;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.config.WMqConfig;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.clients.ClientInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.domain.listener.BrokerRegistryListener;
import com.wentry.wmq.domain.listener.ClientRegistryListener;
import com.wentry.wmq.utils.json.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 * <p>
 * 当前broker的状态
 */
@Component
public class BrokerState implements SmartInitializingSingleton, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(BrokerState.class);

    @Autowired
    ZkRegistry registry;
    @Autowired
    WMqConfig wMqConfig;

    private String brokerId;
    private String clusterName;
    private String host;
    private int port;
    private int lastVersion;
    /**
     * 所有的broker信息
     */
    private Map<String/*brokerId*/, BrokerInfo/*基本的broker信息，用于请求和通信*/> brokerInfoMap = new ConcurrentHashMap<>();

    /**
     * 所有连接的的客户端信息，包括producer和consumer
     */
    private Map<String/*客户端id*/, ClientInfo/*信息*/> clientInfos = new ConcurrentHashMap<>();

    /**
     * 哪些topic及partition的leader
     */
    private Map<String/*topic*/, Set<Integer>/*partitions*/> asLeaderTopicPartitions = new ConcurrentHashMap<>();

    /**
     * 哪些topic及partition的follower
     */
    private Map<String/*topic*/, Set<Integer>/*partitions*/> asFollowerTopicPartitions = new ConcurrentHashMap<>();

    /**
     * 存储follower节点，用于发送replicate
     */
    private Map<String/*topic*/, Map<Integer/*partition*/, Set<String>>/*followers*/> topicPartitionsFollowers = new ConcurrentHashMap<>();

    /**
     * 分区信息，用于replica
     */
    private Map<String/*topic*/, Map<Integer/*partition*/, BrokerInfo/*leaderInfo*/>> topicPartitionLeader = new ConcurrentHashMap<>();

    /**
     * 版本号，保证单向更新
     */
    private long partitionVersion;

    /**
     * ISR
     * 最后一条指令，完全同步的follower合集
     */
    private Map<String/*topic*/, Map<Integer/*partition*/, IsrSet>> isr = new ConcurrentHashMap();



    public long getPartitionVersion() {
        return partitionVersion;
    }

    public BrokerState setPartitionVersion(long partitionVersion) {
        this.partitionVersion = partitionVersion;
        return this;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public BrokerState setBrokerId(String brokerId) {
        this.brokerId = brokerId;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public BrokerState setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getHost() {
        return host;
    }

    public BrokerState setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public BrokerState setPort(int port) {
        this.port = port;
        return this;
    }

    public Map<String, Set<Integer>> getAsFollowerTopicPartitions() {
        return asFollowerTopicPartitions;
    }

    public BrokerState setAsFollowerTopicPartitions(Map<String, Set<Integer>> asFollowerTopicPartitions) {
        this.asFollowerTopicPartitions = asFollowerTopicPartitions;
        return this;
    }

    public Map<String, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public BrokerState setBrokerInfoMap(Map<String, BrokerInfo> brokerInfoMap) {
        this.brokerInfoMap = brokerInfoMap;
        return this;
    }

    public Map<String, Set<Integer>> getAsLeaderTopicPartitions() {
        return asLeaderTopicPartitions;
    }

    public BrokerState setAsLeaderTopicPartitions(Map<String, Set<Integer>> asLeaderTopicPartitions) {
        this.asLeaderTopicPartitions = asLeaderTopicPartitions;
        return this;
    }

    public int getLastVersion() {
        return lastVersion;
    }

    public BrokerState setLastVersion(int lastVersion) {
        this.lastVersion = lastVersion;
        return this;
    }

    public Map<String, Map<Integer, IsrSet>> getIsr() {
        return isr;
    }

    public BrokerState setIsr(Map<String, Map<Integer, IsrSet>> isr) {
        this.isr = isr;
        return this;
    }

    public Map<String, ClientInfo> getClientInfos() {
        return clientInfos;
    }

    public BrokerState setClientInfos(Map<String, ClientInfo> clientInfos) {
        this.clientInfos = clientInfos;
        return this;
    }

    public Map<String, Map<Integer, Set<String>>> getTopicPartitionsFollowers() {
        return topicPartitionsFollowers;
    }

    public BrokerState setTopicPartitionsFollowers(Map<String, Map<Integer, Set<String>>> topicPartitionsFollowers) {
        this.topicPartitionsFollowers = topicPartitionsFollowers;
        return this;
    }

    public void updateLocalPartitions(PartitionSyncReq partitionSyncReq) {
        if (getPartitionVersion() >= partitionSyncReq.getLastVersion()) {
            return;
        }
        setAsLeaderTopicPartitions(pickCurrAsLeaderTopicPartition(partitionSyncReq.getPartitions()));
        setAsFollowerTopicPartitions(pickCurrAsFollowerTopicPartition(partitionSyncReq.getPartitions()));
        setTopicPartitionsFollowers(groupTopicPartitionFollowers(partitionSyncReq.getPartitions()));
        setTopicPartitionLeader(pickTopicPartitionLeader(partitionSyncReq.getPartitions()));

        this.partitionVersion = partitionSyncReq.getLastVersion();
    }

    private Map<String, Map<Integer, BrokerInfo>> pickTopicPartitionLeader(List<PartitionInfo> partitions) {
        Map<String, Map<Integer, BrokerInfo>> newPartitions = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, BrokerInfo> partitionLeader = newPartitions.computeIfAbsent(partition.getTopic(), s -> new ConcurrentHashMap<>());
            for (Map.Entry<Integer, String> ety : partition.getPartitionLeader().entrySet()) {
                partitionLeader.put(ety.getKey(), brokerInfoMap.get(ety.getValue()));
            }
        }
        return newPartitions;
    }

    private Map<String, Map<Integer, Set<String>>> groupTopicPartitionFollowers(List<PartitionInfo> partitions) {
        Map<String, Map<Integer, Set<String>>> res = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, Set<String>> partitionFollowers = partition.getPartitionFollowers();
            res.put(partition.getTopic(), partitionFollowers);
        }
        return res;
    }

    private Map<String, Set<Integer>> pickCurrAsLeaderTopicPartition(List<PartitionInfo> partitions) {
        Map<String, Set<Integer>> res = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, String> partitionLeader = partition.getPartitionLeader();
            for (Map.Entry<Integer, String> ety : partitionLeader.entrySet()) {
                if (ety.getValue().equals(getBrokerId())) {
                    res.computeIfAbsent(partition.getTopic(), s -> new HashSet<>()).add(ety.getKey());
                }
            }
        }
        return res;
    }

    private Map<String, Set<Integer>> pickCurrAsFollowerTopicPartition(List<PartitionInfo> partitions) {
        Map<String, Set<Integer>> res = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, Set<String>> partitionFollowers = partition.getPartitionFollowers();
            for (Map.Entry<Integer, Set<String>> ety : partitionFollowers.entrySet()) {
                if (ety.getValue().contains(getBrokerId())) {
                    res.computeIfAbsent(partition.getTopic(), s -> new HashSet<>()).add(ety.getKey());
                }
            }
        }
        return res;
    }

    public void broadcastPartitionChange(List<PartitionInfo> partitions) {
        PartitionSyncReq req = new PartitionSyncReq().setPartitions(partitions).setLastVersion(getPartitionVersion() + 10);
        for (BrokerInfo broker : getBrokerInfoMap().values()) {
            PartitionSyncResp res = HttpUtils.post(UrlUtils.getPartitionsSyncUrl(broker), req, PartitionSyncResp.class);
            log.info("partition sync res:{}, broker:{}", res, broker);
        }
        for (Map.Entry<String, ClientInfo> ety : getClientInfos().entrySet()) {
            PartitionSyncResp res = HttpUtils.post(UrlUtils.getPartitionsSyncUrl(ety.getValue()), req, PartitionSyncResp.class);
            log.info("partition sync res:{}, client:{}", res, ety.getValue());
        }
    }

    @Override
    public void afterSingletonsInstantiated() {

        //初始化本地状态
        initFromLocal();

        //从注册中心拉取信息初始化
        initFromRegistry();

        //注册zk节点监听事件，实时更新状态
        initListener();

        //注册自身节点到registry
        registrySelf();


        //加钩子
        addShutDownHook();
    }

    private void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                destroy();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }


    List<Closeable> listeners = new ArrayList<>();

    private void initListener() {
        //broker节点变更监听
        listeners.add(registry.addWatch(
                ZkPaths.getBrokerRegistryPath(getClusterName()),
                new BrokerRegistryListener(this, registry)
        ));

        //客户端节点变更监听
        listeners.add(registry.addWatch(
                ZkPaths.getClientRegistryPath(getClusterName()),
                new ClientRegistryListener(this, registry)
        ));

        //topic新增监听
        listeners.add(registry.addWatch(
                ZkPaths.getTopicRegistrySignal(getClusterName()),
                new TopicRegistryListener(this, registry)
        ));
    }

    private void registrySelf() {
        registry.registryBroker(new BrokerInfo()
                .setBrokerId(wMqConfig.getBroker_brokerId())
                .setClusterName(wMqConfig.getBroker_clusterName())
                .setHost(wMqConfig.getBroker_host())
                .setPort(wMqConfig.getBroker_port())
        );
    }

    private void initFromRegistry() {
        List<ClientInfo> clientInfos = registry.getPathChildrenValues(
                ZkPaths.getClientRegistryPath(getClusterName()), ClientInfo.class
        );
        Map<String, ClientInfo> clientMap = clientInfos.stream().collect(
                Collectors.toMap(ClientInfo::getId, x -> x, (o, n) -> n)
        );
        log.info("client fetched :{}", JsonUtils.toJson(clientMap));
        setClientInfos(clientMap);

        List<PartitionInfo> partitions = registry.getPathChildrenValues(
                ZkPaths.getPartitionRegistryPath(getClusterName()), PartitionInfo.class
        );
    }

    private void initFromLocal() {
        setBrokerId(wMqConfig.getBroker_brokerId());
        setClusterName(wMqConfig.getBroker_clusterName());
        setHost(wMqConfig.getBroker_host());
        setPort(wMqConfig.getBroker_port());
    }

    public Map<String, Map<Integer, BrokerInfo>> getTopicPartitionLeader() {
        return topicPartitionLeader;
    }

    public BrokerState setTopicPartitionLeader(Map<String, Map<Integer, BrokerInfo>> topicPartitionLeader) {
        this.topicPartitionLeader = topicPartitionLeader;
        return this;
    }

    @Override
    public void destroy() throws Exception {
        for (Closeable listener : listeners) {
            try {
                if (registry.isStart()) {
                    log.info("closing listener:{}", listener);
                    listener.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        registry.unRegistryBroker(new BrokerInfo()
                .setBrokerId(wMqConfig.getBroker_brokerId())
                .setClusterName(wMqConfig.getBroker_clusterName())
                .setHost(wMqConfig.getBroker_host())
                .setPort(wMqConfig.getBroker_port()));
    }
}
