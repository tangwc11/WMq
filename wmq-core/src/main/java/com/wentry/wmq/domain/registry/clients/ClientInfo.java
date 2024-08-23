package com.wentry.wmq.domain.registry.clients;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 *
 * 客户端信息， producer 或者 consumer
 */
public class ClientInfo implements Serializable {
    private static final long serialVersionUID = -5238337760441906449L;

    public static final String TYPE_PRODUCER = "producer";
    public static final String TYPE_CONSUMER = "consumer";
    private String type;
    private String id;
    private String host;
    private int port;


    public String getType() {
        return type;
    }

    public ClientInfo setType(String type) {
        this.type = type;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClientInfo setId(String id) {
        this.id = id;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ClientInfo setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ClientInfo setPort(int port) {
        this.port = port;
        return this;
    }

    @JsonIgnore
    public boolean isProducer() {
        return TYPE_PRODUCER.equals(getType());
    }

    @JsonIgnore
    public boolean isConsumer() {
        return TYPE_CONSUMER.equals(getType());
    }

}
