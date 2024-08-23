package com.wentry.wmq.model;

import com.wentry.wmq.domain.registry.brokers.BrokerInfo;

import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 */
public class CompareResult {


    List<BrokerInfo> lackBrokers;
    List<BrokerInfo> newBrokers;
    List<BrokerInfo> allBrokers;

    public List<BrokerInfo> getAllBrokers() {
        return allBrokers;
    }

    public CompareResult setAllBrokers(List<BrokerInfo> allBrokers) {
        this.allBrokers = allBrokers;
        return this;
    }

    public List<BrokerInfo> getLackBrokers() {
        return lackBrokers;
    }

    public CompareResult setLackBrokers(List<BrokerInfo> lackBrokers) {
        this.lackBrokers = lackBrokers;
        return this;
    }

    public List<BrokerInfo> getNewBrokers() {
        return newBrokers;
    }

    public CompareResult setNewBrokers(List<BrokerInfo> newBrokers) {
        this.newBrokers = newBrokers;
        return this;
    }
}
