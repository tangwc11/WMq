package com.wentry.wmq.domain.registry.zookeeper;

import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.config.WMqConfig;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.common.Closable;
import com.wentry.wmq.temp.TestListener;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Description:
 * @Author: tangwc
 * 默认注册中心的实现 zookeeper，封装curator
 * <p>
 * 借助zk，实现了CP
 */
public class ZookeeperRegistryImpl implements ZkRegistry, Closable {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperRegistryImpl.class);

    private final WMqConfig wMqConfig;
    private CuratorFramework client;

    public ZookeeperRegistryImpl(WMqConfig wMqConfig) {
        this.wMqConfig = wMqConfig;
        initCurator();
    }

    private void initCurator() {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(wMqConfig.getCommon_registryAddress())
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        // 启动客户端
        this.client.start();
    }


    @Override
    public void registryBroker(BrokerInfo brokerInfo) {
        /**
         * 每个brokerId注册一个目录，不同的brokerId互不冲突，重复的brokerId被覆盖
         */
        String json = Objects.requireNonNull(JsonUtils.toJson(brokerInfo));
        String path = ZkPaths.getBrokerRegistryNodePath(brokerInfo.getClusterName(), brokerInfo.getBrokerId());
        try {

            log.info("Creating node at path: {}", path);
            log.info("Data to be stored: {}", json);

            updateVal(CreateMode.EPHEMERAL, path, brokerInfo);

        } catch (Exception e) {
            log.error("Failed to register broker in ZooKeeper with path: {} and data: {}", path, json, e);
        }
    }

    @Override
    public <T> boolean updateVal(CreateMode mode, String path, T val) {
        try {
            if (nodeExists(path)) {
                // 如果节点存在，则更新节点数据
                byte[] data = JsonUtils.toJson(val).getBytes(StandardCharsets.UTF_8);
                client.setData().forPath(path, data);
            } else {
                // 如果节点不存在，则创建节点
                client.create().creatingParentsIfNeeded()
                        .withMode(mode)
                        .forPath(path, JsonUtils.toJson(val).getBytes(StandardCharsets.UTF_8));
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean nodeExists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void unRegistryBroker(BrokerInfo brokerInfo) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(
                    ZkPaths.getBrokerRegistryNodePath(brokerInfo.getClusterName(), brokerInfo.getBrokerId())
            );
        } catch (Exception e) {
            log.warn("ZookeeperRegistryImpl.unRegistryBroker("+"brokerInfo = " + brokerInfo+")");
        }
    }

    @Override
    public void deletePath(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            log.warn("ZookeeperRegistryImpl.deletePath("+"path = " + path+")");
        }
    }

    @Override
    public List<BrokerInfo> getBrokers(String clusterName) {
        return getPathChildrenValues(ZkPaths.getBrokerRegistryPath(clusterName), BrokerInfo.class);
    }

    @Override
    public <T> T getPathValue(String path, Class<T> clz, T defaultVal) {
        try {
            Stat stat = checkExist(path);
            if (stat == null) {
                return defaultVal;
            }
            byte[] bytes = client.getData().forPath(path);
            return JsonUtils.parseJson(new String(bytes, StandardCharsets.UTF_8), clz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultVal;
    }


    @Override
    public <T> T getPathValue(String path, Class<T> clz) {
        try {
            Stat stat = checkExist(path);
            if (stat == null) {
                return null;
            }
            byte[] bytes = client.getData().forPath(path);
            return JsonUtils.parseJson(new String(bytes, StandardCharsets.UTF_8), clz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T> List<T> getPathChildrenValues(String path, Class<T> clz) {
        try {

            Stat stat = checkExist(path);
            if (stat == null) {
                log.warn("path emp :{}", path);
                return new ArrayList<>();
            }

            List<String> children = client.getChildren().forPath(path);
            if (CollectionUtils.isEmpty(children)) {
                log.warn("child emp for path:{}", path);
                return new ArrayList<>();
            }

            List<T> brokers = new ArrayList<>();
            for (String child : children) {
                T obj = getPathValue(ZkPaths.join(path, child), clz);
                if (obj == null) {
                    log.info("none val for node :{}", child);
                    continue;
                }
                brokers.add(obj);
            }
            return brokers;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public NodeCache addWatch(String path, NodeCacheListener listener) {
        try {
            // 创建 NodeCache 实例
            NodeCache nodeCache = new NodeCache(client, path);

            // 添加 NodeCache 监听器
            nodeCache.getListenable().addListener(listener);

            // 开始缓存
            nodeCache.start();

            return nodeCache;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public PathChildrenCache addWatch(String path, PathChildrenCacheListener listener) {
        try {
            // 创建 PathChildrenCache 实例
            PathChildrenCache pathCache = new PathChildrenCache(client, path, true);

            // 添加 PathChildrenCache 监听器
            pathCache.getListenable().addListener(listener);

            // 开始缓存
            pathCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            return pathCache;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Stat checkExist(String path) {
        try {
            return client.checkExists().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Stat();
    }

    @Override
    public boolean isStart() {
        return client.isStarted();
    }

    @Override
    public void close() {
//        if (client != null) {
//            client.close();
//        }
    }

    @Override
    public int order() {
        return 10;
    }

    public static void main(String[] args) throws InterruptedException {
        ZookeeperRegistryImpl zk = new ZookeeperRegistryImpl(new WMqConfig().setCommon_registryAddress("localhost:2181"));

        BrokerInfo brokerInfo = new BrokerInfo().setBrokerId("broker-1").setPort(100).setHost("localhost").setClusterName("clusterName");
        zk.registryBroker(brokerInfo);


        zk.addWatch(ZkPaths.getBrokerRegistryPath(brokerInfo.getClusterName()), new TestListener(brokerInfo.getClusterName(), zk));

        Thread.sleep(1000000);
    }
}
