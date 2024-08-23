package com.wentry.wmq.listener;

import com.wentry.wmq.domain.ProducerState;
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
public class ProducerBrokerRegistryListener implements PathChildrenCacheListener {


    private final ZkRegistry registry;
    private final ProducerState producerState;

    public ProducerBrokerRegistryListener(ZkRegistry registry, ProducerState producerState) {
        this.registry = registry;
        this.producerState = producerState;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        List<BrokerInfo> brokerInfo = registry.getBrokers(producerState.getClusterName());
        producerState.setBrokerInfoMap(brokerInfo.stream().collect(Collectors.toMap(BrokerInfo::getBrokerId, o -> o)));
    }
}
