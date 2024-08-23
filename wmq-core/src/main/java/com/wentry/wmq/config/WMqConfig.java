package com.wentry.wmq.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @Description:
 * @Author: tangwc
 */
@Configuration
@ConfigurationProperties(prefix = "wmq")
public class WMqConfig {

    private String broker_clusterName = "testCluster";
    private String broker_brokerId = "broker-1";
    private String broker_host = "localhost";
    //等同于server.port配置
    @Value("${server.port}")
    private int broker_port;

    private String common_registryAddress = "localhost:2181";

    private String producer_clusterName = "testCluster";
    private String producer_host = "localhost";
    @Value("${server.port}")
    private int producer_port;

    private String consumer_clusterName = "testCluster";
    private String consumer_host = "localhost";
    @Value("${server.port}")
    private int consumer_port;

    private String adminCluster = "testCluster";

    public String getAdminCluster() {
        return adminCluster;
    }

    public WMqConfig setAdminCluster(String adminCluster) {
        this.adminCluster = adminCluster;
        return this;
    }

    public String getConsumer_clusterName() {
        return consumer_clusterName;
    }

    public WMqConfig setConsumer_clusterName(String consumer_clusterName) {
        this.consumer_clusterName = consumer_clusterName;
        return this;
    }

    public String getConsumer_host() {
        return consumer_host;
    }

    public WMqConfig setConsumer_host(String consumer_host) {
        this.consumer_host = consumer_host;
        return this;
    }

    public int getConsumer_port() {
        return consumer_port;
    }

    public WMqConfig setConsumer_port(int consumer_port) {
        this.consumer_port = consumer_port;
        return this;
    }

    public String getBroker_host() {
        return broker_host;
    }

    public WMqConfig setBroker_host(String broker_host) {
        this.broker_host = broker_host;
        return this;
    }

    public int getBroker_port() {
        return broker_port;
    }

    public WMqConfig setBroker_port(int broker_port) {
        this.broker_port = broker_port;
        return this;
    }

    public String getCommon_registryAddress() {
        return common_registryAddress;
    }

    public WMqConfig setCommon_registryAddress(String common_registryAddress) {
        this.common_registryAddress = common_registryAddress;
        return this;
    }

    public String getBroker_clusterName() {
        return broker_clusterName;
    }

    public WMqConfig setBroker_clusterName(String broker_clusterName) {
        this.broker_clusterName = broker_clusterName;
        return this;
    }

    public String getBroker_brokerId() {
        return broker_brokerId;
    }

    public WMqConfig setBroker_brokerId(String broker_brokerId) {
        this.broker_brokerId = broker_brokerId;
        return this;
    }

    public String getProducer_clusterName() {
        return producer_clusterName;
    }

    public WMqConfig setProducer_clusterName(String producer_clusterName) {
        this.producer_clusterName = producer_clusterName;
        return this;
    }

    public String getProducer_host() {
        return producer_host;
    }

    public WMqConfig setProducer_host(String producer_host) {
        this.producer_host = producer_host;
        return this;
    }

    public int getProducer_port() {
        return producer_port;
    }

    public WMqConfig setProducer_port(int producer_port) {
        this.producer_port = producer_port;
        return this;
    }

    @Override
    public String toString() {
        return "WMqConfig{" +
                "broker_clusterName='" + broker_clusterName + '\'' +
                ", broker_brokerId='" + broker_brokerId + '\'' +
                ", broker_host='" + broker_host + '\'' +
                ", broker_port=" + broker_port +
                ", common_registryAddress='" + common_registryAddress + '\'' +
                ", producer_clusterName='" + producer_clusterName + '\'' +
                ", producer_host='" + producer_host + '\'' +
                ", producer_port=" + producer_port +
                ", consumer_clusterName='" + consumer_clusterName + '\'' +
                ", consumer_host='" + consumer_host + '\'' +
                ", consumer_port=" + consumer_port +
                ", adminCluster='" + adminCluster + '\'' +
                '}';
    }
}
