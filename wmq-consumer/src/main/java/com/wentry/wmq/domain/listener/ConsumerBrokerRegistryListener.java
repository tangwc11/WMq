package com.wentry.wmq.domain.listener;

import com.wentry.wmq.domain.ConsumerState;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
public class ConsumerBrokerRegistryListener implements PathChildrenCacheListener {


    public ConsumerBrokerRegistryListener(ConsumerState consumerState, ZkRegistry registry) {
        this.consumerState = consumerState;
        this.registry = registry;
    }

    ConsumerState consumerState;
    ZkRegistry registry;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        List<BrokerInfo> brokerInfo = registry.getBrokers(consumerState.getClusterName());
        consumerState.setBrokerInfoMap(brokerInfo.stream().collect(Collectors.toMap(BrokerInfo::getBrokerId, o -> o)));
    }
}
