package com.wentry.wmq.domain.listener;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.clients.ClientInfo;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
public class ClientRegistryListener implements PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(ClientRegistryListener.class);

    private final BrokerState brokerState;
    private final ZkRegistry registry;

    public ClientRegistryListener(BrokerState brokerState, ZkRegistry registry) {
        this.brokerState = brokerState;
        this.registry = registry;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        List<ClientInfo> clientInfos = registry.getPathChildrenValues(
                ZkPaths.getClientRegistryPath(brokerState.getClusterName()), ClientInfo.class
        );
        Map<String, ClientInfo> clientMap = clientInfos.stream().collect(
                Collectors.toMap(ClientInfo::getId, x -> x, (o, n) -> n)
        );
        log.info("client changed :{}", JsonUtils.toJson(clientMap));
        brokerState.setClientInfos(clientMap);
    }
}
