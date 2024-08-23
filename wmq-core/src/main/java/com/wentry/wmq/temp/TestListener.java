package com.wentry.wmq.temp;

import com.wentry.wmq.utils.zk.ZkPaths;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.zookeeper.ZookeeperRegistryImpl;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 */
public class TestListener implements PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(TestListener.class);

    String clusterName;
    ZookeeperRegistryImpl zk;

    public TestListener(String clusterName, ZookeeperRegistryImpl zk) {
        this.clusterName = clusterName;
        this.zk = zk;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String path = ZkPaths.getBrokerRegistryPath(clusterName);
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                System.out.println("Node changed!");

                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    log.warn("stat null for path:{}", path);
                    return;
                }
                System.out.println(stat + ":" + JsonUtils.toJson(stat));
                List<BrokerInfo> brokers = zk.getBrokers(clusterName);
                System.out.println("brokers:" + JsonUtils.toJson(brokers));
                break;
            case CHILD_REMOVED:
                System.out.println("Node removed!");
                break;
            default:
                System.out.println("Other event type: " + event.getType());
        }
    }

}
