package com.wentry.wmq.domain.registry.clients;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ConsumerInstanceInfo implements Serializable {
    private static final long serialVersionUID = 157736970395343254L;

    String consumerId;
    String host;
    int port;
    int partition;

    public int getPartition() {
        return partition;
    }

    public ConsumerInstanceInfo setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ConsumerInstanceInfo setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ConsumerInstanceInfo setPort(int port) {
        this.port = port;
        return this;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public ConsumerInstanceInfo setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        return this;
    }
}
