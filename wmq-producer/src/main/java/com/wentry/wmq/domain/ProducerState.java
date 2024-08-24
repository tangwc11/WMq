package com.wentry.wmq.domain;

import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.config.WMqConfig;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.clients.ClientInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.listener.ProducerBrokerRegistryListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 * 状态维护
 */
@Component
public class ProducerState implements SmartInitializingSingleton , DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ProducerState.class);

    @Autowired
    ZkRegistry registry;

    @Autowired
    WMqConfig wMqConfig;


    private String clusterName;
    private String producerId;
    private String host;
    private int port;

    private Map<String, BrokerInfo> brokerInfoMap = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, BrokerInfo>> partitions = new ConcurrentHashMap<>();
    private long partitionVersion;

    public BrokerInfo getLeader(String topic, int partition) {
        return partitions.get(topic).get(partition);
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public ProducerState setPartitionVersion(long partitionVersion) {
        this.partitionVersion = partitionVersion;
        return this;
    }

    public void updatePartitions(PartitionSyncReq partitionSyncReq) {
        try {
            if (getPartitionVersion() >= partitionSyncReq.getLastVersion()) {
                return;
            }
            Map<String, Map<Integer, BrokerInfo>> newPartitions = new ConcurrentHashMap<>();
            for (PartitionInfo partition : partitionSyncReq.getPartitions()) {
                Map<Integer, BrokerInfo> partitionLeader = newPartitions.computeIfAbsent(partition.getTopic(), s -> new ConcurrentHashMap<>());
                for (Map.Entry<Integer, String> ety : partition.getPartitionLeader().entrySet()) {
                    BrokerInfo brokerInfo = brokerInfoMap.get(ety.getValue());
                    if (brokerInfo == null) {
                        List<BrokerInfo> brokerInfoList = registry.getBrokers(getClusterName());
                        setBrokerInfoMap(brokerInfoList.stream().collect(Collectors.toMap(BrokerInfo::getBrokerId, o -> o)));
                        brokerInfo = brokerInfoMap.get(ety.getValue());
                    }
                    if (brokerInfo == null) {
                        log.error("brokerInfo still null, brokerInfoMap is:{},partitionSyncReq:{}", JsonUtils.toJson(brokerInfo), JsonUtils.toJson(partitionSyncReq));
                    }
                    partitionLeader.put(ety.getKey(), brokerInfo);
                }
            }
            this.partitions = newPartitions;
            this.partitionVersion = partitionSyncReq.getLastVersion();
        } catch (Exception e) {
            log.error("ProducerState.updatePartitions(" + "partitionSyncReq = " + JsonUtils.toJson(partitionSyncReq) + ")", e);
        }
    }

    public ZkRegistry getRegistry() {
        return registry;
    }

    public ProducerState setRegistry(ZkRegistry registry) {
        this.registry = registry;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ProducerState setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getProducerId() {
        return producerId;
    }

    public ProducerState setProducerId(String producerId) {
        this.producerId = producerId;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ProducerState setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ProducerState setPort(int port) {
        this.port = port;
        return this;
    }

    public Map<String, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public Map<String, Map<Integer, BrokerInfo>> getPartitions() {
        return partitions;
    }

    public ProducerState setPartitions(Map<String, Map<Integer, BrokerInfo>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public ProducerState setBrokerInfoMap(Map<String, BrokerInfo> brokerInfoMap) {
        this.brokerInfoMap = brokerInfoMap;
        return this;
    }

    @Override
    public void afterSingletonsInstantiated() {
        initFromConfig();

        //拉取zk信息
        initFromRegistry();

        //注册监听器
        initListener();

        //注册自身节点到zk
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

    private void initListener() {
        /**
         * 1。监听broker的变化，更新brokerInfoMap
         */
        listeners.add(registry.addWatch(
                ZkPaths.getBrokerRegistryPath(getClusterName()),
                new ProducerBrokerRegistryListener(registry, this)
        ));
    }

    private void registrySelf() {
        registry.updateVal(
                CreateMode.EPHEMERAL,
                ZkPaths.getClientRegistryPathNode(getClusterName(), "PRODUCER:" + getProducerId()),
                new ClientInfo()
                        .setType(ClientInfo.TYPE_PRODUCER)
                        .setHost(getHost())
                        .setPort(getPort())
                        .setId(getProducerId())
        );
    }

    private void initFromConfig() {
        //填充本地配置信息
        setProducerId(UUID.randomUUID().toString());
        setHost(wMqConfig.getProducer_host());
        setPort(wMqConfig.getProducer_port());
        setClusterName(wMqConfig.getProducer_clusterName());
    }

    private void initFromRegistry() {
        List<BrokerInfo> brokerInfo = registry.getBrokers(this.getClusterName());
        setBrokerInfoMap(brokerInfo.stream().collect(Collectors.toMap(BrokerInfo::getBrokerId, o -> o)));

        List<PartitionInfo> partitions = registry.getPathChildrenValues(
                ZkPaths.getPartitionRegistryPath(getClusterName()), PartitionInfo.class
        );
        Map<String, Map<Integer, BrokerInfo>> newPartitions = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, BrokerInfo> partitionLeader = newPartitions.computeIfAbsent(partition.getTopic(), s -> new ConcurrentHashMap<>());
            for (Map.Entry<Integer, String> ety : partition.getPartitionLeader().entrySet()) {
                partitionLeader.put(ety.getKey(), brokerInfoMap.get(ety.getValue()));
            }
        }
        this.partitions = newPartitions;
    }

    List<Closeable> listeners = new ArrayList<>();

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
        registry.deletePath(ZkPaths.getClientRegistryPathNode(getClusterName(), "PRODUCER:" + getProducerId()));
    }
}
