package com.wentry.wmq.domain.registry.brokers;

import java.util.Objects;

/**
 * @Description:
 * @Author: tangwc
 */
public class BrokerInfo {

    private String brokerId;
    private String host;
    private int port;
    private String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public BrokerInfo setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public BrokerInfo setBrokerId(String brokerId) {
        this.brokerId = brokerId;
        return this;
    }

    public String getHost() {
        return host;
    }

    public BrokerInfo setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public BrokerInfo setPort(int port) {
        this.port = port;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokerInfo that = (BrokerInfo) o;
        return port == that.port && Objects.equals(brokerId, that.brokerId) && Objects.equals(host, that.host) && Objects.equals(clusterName, that.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, host, port, clusterName);
    }

}
