package com.wentry.wmq.domain;

import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: tangwc
 */
public class TempBrokerState {

    private boolean needNotify;
    private String msg;
    private List<PartitionInfo> partitions;
    private Map<String, BrokerInfo> brokerInfoMap;

    public boolean isNeedNotify() {
        return needNotify;
    }

    public TempBrokerState setNeedNotify(boolean needNotify) {
        this.needNotify = needNotify;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public TempBrokerState setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public TempBrokerState setPartitions(List<PartitionInfo> partitions) {
        this.partitions = partitions;
        return this;
    }

    public Map<String, BrokerInfo> getBrokerInfoMap() {
        return brokerInfoMap;
    }

    public TempBrokerState setBrokerInfoMap(Map<String, BrokerInfo> brokerInfoMap) {
        this.brokerInfoMap = brokerInfoMap;
        return this;
    }

    @Override
    public String toString() {
        return "NewBrokerState{" +
                "needNotify=" + needNotify +
                ", msg='" + msg + '\'' +
                ", partitions=" + partitions +
                ", brokerInfoMap=" + brokerInfoMap +
                '}';
    }
}
