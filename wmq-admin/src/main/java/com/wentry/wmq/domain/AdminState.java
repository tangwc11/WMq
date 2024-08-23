package com.wentry.wmq.domain;

import com.wentry.wmq.config.WMqConfig;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.rmi.registry.Registry;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class AdminState implements SmartInitializingSingleton {

    @Autowired
    ZkRegistry registry;

    @Autowired
    WMqConfig wMqConfig;

    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public AdminState setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    @Override
    public void afterSingletonsInstantiated() {
        setClusterName(wMqConfig.getConsumer_clusterName());
    }

}
