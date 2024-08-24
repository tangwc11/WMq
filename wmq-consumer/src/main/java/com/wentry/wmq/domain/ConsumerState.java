package com.wentry.wmq.domain;

import com.wentry.wmq.domain.listener.ConsumerInstanceUnRegistryListener;
import com.wentry.wmq.openapi.WmqListener;
import com.wentry.wmq.domain.registry.clients.ConsumerInstanceInfo;
import com.wentry.wmq.domain.registry.offset.OffsetInfo;
import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.transport.ReBalanceReq;
import com.wentry.wmq.transport.ReBalanceResp;
import com.wentry.wmq.utils.MixUtils;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.config.WMqConfig;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.clients.ClientInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;
import com.wentry.wmq.domain.listener.ConsumerBrokerRegistryListener;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class ConsumerState implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(ConsumerState.class);

    @Autowired
    WMqConfig wMqConfig;

    @Autowired
    ZkRegistry registry;

    private String clusterName;
    private String consumerId = UUID.randomUUID().toString();
    private String host;
    private int port;

    private Map<String, BrokerInfo> brokerInfoMap = new ConcurrentHashMap<>();
    private Map<String, Map<Integer, BrokerInfo>> partitions = new ConcurrentHashMap<>();
    private long partitionVersion;
    private ApplicationContext ctx;

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public ConsumerState setPartitionVersion(long partitionVersion) {
        this.partitionVersion = partitionVersion;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ConsumerState setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public ConsumerState setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ConsumerState setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ConsumerState setPort(int port) {
        this.port = port;
        return this;
    }

    public Map<String, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public ConsumerState setBrokerInfoMap(Map<String, BrokerInfo> brokerInfoMap) {
        this.brokerInfoMap = brokerInfoMap;
        return this;
    }

    public Map<String, Map<Integer, BrokerInfo>> getPartitions() {
        return partitions;
    }

    public ConsumerState setPartitions(Map<String, Map<Integer, BrokerInfo>> partitions) {
        this.partitions = partitions;
        return this;
    }

    @Override
    public void afterSingletonsInstantiated() {
        //加载配置
        initFromConfig();

        //加载zk信息
        initFromRegistry();

        //zk监听器
        initListener();

        //注册自身节点到zk
        registrySelf();

        //初始化消息监听器
        initWmqListener();

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

    @Autowired
    List<WmqListener> listeners;

    private void initWmqListener() {
        /**
         * 首先，consumer是动态的，在上下线的过程中，有re-balance的过程
         * 1. 新上线一个consumer，对应的group里其余的consumer实例需要让渡至少一个partition出来，否则新上线的consumer将处于饥饿状态。
         * 2. 每个topic消费的维度是 ,topic-partition-group : offset，和consumer实例其实没关系，consumer实例相当于group的执行基本
         * 单位，多个consumer组成一个group进行消费。
         *
         * 再来捋一下一个consumer实例的上线过程
         * 1. consumer定义topic、group、id进行启动
         * 2. 会创建临时节点
         *     client-registry：客户端信息
         * 3. 拿到topic的分区：创建以下临时节点
         *     topic-group-partition：客户端信息
         * 4. 如果不能创建，说明已经有别的实例消费了此分区了
         * 5. 每次上线consumer，将会尝试创建所有的partition，比如4个分区，第一个上线的consumer，将会创建4个节点，也就是消费4个分区
         * 6. 当第二个节点上线时，将会进行partition分担也就是re-balance，re-balance算法这里简单点搞，就是随机从实例中挑选几个
         * 7. 切换分区的过程，每个consumer实例需要提供接口，过程分为以下几步：
         *       a. 新的consumer实例A，根据一定的算法，选取需要负责的分区，并根据分区找到目前正在消费的consumer实例B
         *       b. 调用B的让渡接口，此时B对partition进行停止消费，并保存offset（这里处理的不好就会有重复消费问题）
         *       c. 响应成功之后，A修改zk节点的信息为自己，并接力B进行消费（这里必须保证成功，不然停掉B之后自己又没消费，这个partition就废了）
         *       d. 拉模式，都是consumer拉取broker，因此partition-consumer的对应关系，不需要通知broker，也就不需要后续操作了
         *       e. offset信息最好是另外开节点存储，如果存在topic-group-partition里面，可能引起混乱，且topic-group-partition会随着上下线销毁
         * 8. 结合上一步的分析，offset消费进度单独开节点存储，路径为： topic-group-partition，且为永久节点！！！（这个消息不能丢，只能更新）
         * 9. 为了提升性能，offset不一定要+1的递增，可以一次取N个，消费完之后再add（又会引入重复消费的问题，处处都是细节啊！！）
         */

        /**
         * topic要先创建，才能再启动consumer和producer，为了简化处理，broker也需要等topic先创建再启动
         * topic的基本信息在节点：topic-registry下面
         * 启动顺序：
         * 1。 topic创建
         * 2。 broker启动
         * 3。 producer启动
         * 4。 consumer启动
         */

        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }

        for (WmqListener listener : listeners) {
            String topic = listener.topic();
            Map<Integer, BrokerInfo> partitionToLeader = partitions.get(topic);
            if (MapUtils.isEmpty(partitionToLeader)) {
                log.info("topic {} ' partitions leader not found", topic);
                continue;
            }
            //去topic-group-partition找，空缺的自己占上，结束
            Set<Integer> partitions = partitionToLeader.keySet();
            List<ConsumerInstanceInfo> cInstInfos = new ArrayList<>();
            for (Integer partition : partitions) {
                String path = ZkPaths.getConsumerInstanceNode(clusterName, topic, listener.consumerGroup(), partition);
                ConsumerInstanceInfo consumerInstanceInfo = registry.getPathValue(
                        path, ConsumerInstanceInfo.class
                );
                log.info("path:{},consumerInstanceInfo:{}", path, JsonUtils.toJson(consumerInstanceInfo));
                if (consumerInstanceInfo != null) {
                    //有consumer实例占据了
                    cInstInfos.add(consumerInstanceInfo);
                } else {
                    //没有实例，自己创建实例
                    getConsumerInstanceMap().put(
                            MixUtils.consumerInstanceKey(topic, partition, listener.consumerGroup()),
                            createConsumerInstance(listener, partition, topic,"init create primary listener")
                    );
                }
            }

            //如果没有空缺的，需要别人让渡自己
            if (MapUtils.isEmpty(getConsumerInstanceMap())) {
                doReBalance(listener, topic, cInstInfos);
            }

            //开启实例消费
            startPrimaryListener(listener, topic);
        }
    }

    private void startPrimaryListener(WmqListener listener, String topic) {
        for (ConsumerInstance inst : getConsumerInstanceMap().values()) {
            if (inst.isStarted()) {
                continue;
            }
            //add node
            boolean res = registry.createNodeWithValue(
                    CreateMode.EPHEMERAL,
                    ZkPaths.getConsumerInstanceNode(clusterName, topic, listener.consumerGroup(), inst.getPartition()),
                    new ConsumerInstanceInfo().setPartition(inst.getPartition()).setHost(getHost()).setPort(getPort()).setConsumerId(getConsumerId())
            );
            if (res) {
                inst.start();
                log.info("started consumer inst:{}", JsonUtils.toJson(inst));
            } else {
                log.info("create node fail for inst:{}", JsonUtils.toJson(inst));
            }
        }

        //清理一遍
        getConsumerInstanceMap().values().removeIf(x -> !x.isStarted());
    }

    private void doReBalance(WmqListener listener, String topic, List<ConsumerInstanceInfo> cInstInfos) {
        // 使用流和分组统计每个元素出现的次数
        Map<String, Long> frequencyMap = cInstInfos.stream()
                .collect(Collectors.groupingBy(ConsumerInstanceInfo::getConsumerId, Collectors.counting()));

        // 找出出现次数最多的元素
        Map.Entry<String, Long> mostCommonEntry = frequencyMap.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow(() -> new RuntimeException("List is empty"));
        Map<String, ConsumerInstanceInfo> map = cInstInfos.stream().collect(
                Collectors.toMap(ConsumerInstanceInfo::getConsumerId, o -> o, (o, n) -> n)
        );
        ConsumerInstanceInfo info = map.get(mostCommonEntry.getKey());
        if (info == null) {
            throw new RuntimeException("none re-balance node found");
        }

        //发起re-balance请求
        ReBalanceResp resp = HttpUtils.post(
                UrlUtils.getConsumerReBalanceUrl(info),
                new ReBalanceReq()
                        .setGroup(listener.consumerGroup())
                        .setPartition(info.getPartition())
                        .setTopic(topic),
                ReBalanceResp.class
        );
        if (resp != null && resp.isSuccess()) {
            createAndStartConsumerInstance(listener, topic, info, resp);
        } else {
            log.error("re-balance fail, msg:{}", JsonUtils.toJson(resp));
        }
    }

    private void createAndStartConsumerInstance(WmqListener listener, String topic, ConsumerInstanceInfo info, ReBalanceResp resp) {
        ConsumerInstance instance = createConsumerInstance(listener, info.getPartition(), topic, "do re-balance")
                //接过re-balance上次的进度继续消费
                .setLastAckOffset(resp.getLastOffset());

        getConsumerInstanceMap().put(
                MixUtils.consumerInstanceKey(topic, info.getPartition(), listener.consumerGroup()),
                instance
        );
        //更新实例信息
        boolean res = registry.createNodeWithValue(
                CreateMode.EPHEMERAL,
                ZkPaths.getConsumerInstanceNode(clusterName, topic, listener.consumerGroup(), info.getPartition()),
                info.setPartition(info.getPartition()).setHost(getHost()).setPort(getPort()).setConsumerId(getConsumerId())
        );

        if (!res) {
            log.error("re-balance update node res:{}, node:{}", res, JsonUtils.toJson(info));
            getConsumerInstanceMap().remove(MixUtils.consumerInstanceKey(topic, info.getPartition(), listener.consumerGroup()));
        } else {
            instance.start();
            log.info("re-balance update node res:{}, node:{}", res, JsonUtils.toJson(info));
        }
    }

    private long fetchLastAckOffset(String topic, String consumerGroup, Integer partition) {

        OffsetInfo offset = registry.getPathValue(
                ZkPaths.getConsumerOffset(getClusterName(), topic, consumerGroup, partition), OffsetInfo.class
        );

        if (offset == null) {
            return 0L;
        }

        return offset.getOffset();
    }

    /**
     * 每个topic-group-partition 对应一个 consumerInstance
     * 每个应用会有多个consumerInstance，里面包含了listener，以及线程池调度等信息
     */
    Map<String, ConsumerInstance> consumerInstanceMap = new ConcurrentHashMap<>();

    public ConsumerInstance createConsumerInstance(WmqListener listener, Integer partition, String topic, String source) {
        ConsumerInstance instance = new ConsumerInstance(this, listener, partition, topic)
                //初始从0开始消费，如果要从最后，需要读取请求broker接口。可以扩展为HEAT或者TAIL
                .setLastAckOffset(fetchLastAckOffset(topic, listener.consumerGroup(), partition));

        log.info("create createConsumerInstance ,topic:{},partition:{},source:{},instance:{}", topic, partition, source, instance);

        return instance;
    }

    private void registrySelf() {
        registry.updateVal(
                CreateMode.EPHEMERAL,
                ZkPaths.getClientRegistryPathNode(getClusterName(), "CONSUMER:" + getConsumerId()),
                new ClientInfo()
                        .setType(ClientInfo.TYPE_CONSUMER)
                        .setHost(getHost())
                        .setPort(getPort())
                        .setId(getConsumerId())
        );
    }

    private void initListener() {
        //监听broker上下线
        zkListeners.add(registry.addWatch(
                ZkPaths.getBrokerRegistryPath(getClusterName()),
                new ConsumerBrokerRegistryListener(this, registry)
        ));

        //监听consumerInstance下线
        zkListeners.add(registry.addWatch(
                ZkPaths.getConsumerInstanceDir(clusterName),
                new ConsumerInstanceUnRegistryListener(this, registry, ctx)
        ));
    }

    private void initFromRegistry() {
        //拉取zk信息
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

    private void initFromConfig() {
        setClusterName(wMqConfig.getConsumer_clusterName());
        setHost(wMqConfig.getConsumer_host());
        setPort(wMqConfig.getConsumer_port());
    }

    public void updatePartitions(PartitionSyncReq partitionSyncReq) {
        if (this.partitionVersion >= partitionSyncReq.getLastVersion()) {
            return;
        }
        List<PartitionInfo> partitions = partitionSyncReq.getPartitions();
        Map<String, Map<Integer, BrokerInfo>> newPartitions = new ConcurrentHashMap<>();
        for (PartitionInfo partition : partitions) {
            Map<Integer, BrokerInfo> partitionLeader = newPartitions.computeIfAbsent(partition.getTopic(), s -> new ConcurrentHashMap<>());
            for (Map.Entry<Integer, String> ety : partition.getPartitionLeader().entrySet()) {
                partitionLeader.put(ety.getKey(), brokerInfoMap.get(ety.getValue()));
            }
        }
        this.partitions = newPartitions;
        this.partitionVersion = partitionSyncReq.getLastVersion();
    }

    public Map<String, ConsumerInstance> getConsumerInstanceMap() {
        return consumerInstanceMap;
    }

    public ConsumerState setConsumerInstanceMap(Map<String, ConsumerInstance> consumerInstanceMap) {
        this.consumerInstanceMap = consumerInstanceMap;
        return this;
    }

    public void reportConsumerOffset(String topic, String consumerGroup, int partition, long updatingOffset) {
        String path = ZkPaths.getConsumerOffset(clusterName, topic, consumerGroup, partition);
        OffsetInfo offset = registry.getPathValue(path, OffsetInfo.class);
        if (offset == null || offset.getOffset() < updatingOffset) {
            registry.updateVal(
                    CreateMode.PERSISTENT,
                    path,
                    new OffsetInfo()
                            .setTopic(topic)
                            .setGroup(consumerGroup)
                            .setPartition(partition)
                            .setOffset(updatingOffset)
            );
//            log.info("update offset from :{} to :{} ,path:{}", JsonUtils.toJson(offset), updatingOffset, path);
        }
    }

    List<Closeable> zkListeners = new ArrayList<>();

    @Override
    public void destroy() throws Exception {
        for (Closeable listener : zkListeners) {
            try {
                if (registry.isStart()) {
                    log.info("closing listener:{}", listener);
                    listener.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        registry.deletePath(ZkPaths.getClientRegistryPathNode(getClusterName(), "CONSUMER:" + getConsumerId()));

        for (Map.Entry<String, ConsumerInstance> consumerInstances : getConsumerInstanceMap().entrySet()) {
            ConsumerInstance instance = consumerInstances.getValue();
            instance.stop(true);
        }

    }


    public static void main(String[] args) {

        List<ConsumerInstanceInfo> cInstInfos = new ArrayList<>();
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("1"));
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("2"));
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("2"));
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("3"));
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("3"));
        cInstInfos.add(new ConsumerInstanceInfo().setConsumerId("3"));

        // 使用流和分组统计每个元素出现的次数
        Map<String, Long> frequencyMap = cInstInfos.stream()
                .collect(Collectors.groupingBy(ConsumerInstanceInfo::getConsumerId, Collectors.counting()));

        // 找出出现次数最多的元素
        Map.Entry<String, Long> mostCommonEntry = frequencyMap.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow(() -> new RuntimeException("List is empty"));


        System.out.println(mostCommonEntry);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.ctx = applicationContext;
    }
}
