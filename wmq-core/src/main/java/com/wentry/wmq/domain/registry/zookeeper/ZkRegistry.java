package com.wentry.wmq.domain.registry.zookeeper;

import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 *
 * 几个职责：
 * 1。 负责broker的partition归属管理
 * 2。 负责集群的broker列表管理，供consumer连接
 * 3。 负责consumer的消费者列表管理，供admin查询
 * 4。
 *
 */
public interface ZkRegistry {

    /**
     * 注册broker信息
     */
    void registryBroker(BrokerInfo brokerInfo);


    <T> boolean updateVal(CreateMode mode, String path, T val);

    /**
     * 注销
     */
    void unRegistryBroker(BrokerInfo brokerInfo);

    void deletePath(String path);

    /**
     * 获取集群的brokers
     */
    List<BrokerInfo> getBrokers(String clusterName);

    <T> T getPathValue(String path, Class<T> clz, T defaultVal);

    <T> T getPathValue(String path, Class<T> clz);

    <T> List<T> getPathChildrenValues(String path, Class<T> clz);

    NodeCache addWatch(String path, NodeCacheListener listener);

    PathChildrenCache addWatch(String path, PathChildrenCacheListener listener);

    Stat checkExist(String path);

    boolean isStart();
}
