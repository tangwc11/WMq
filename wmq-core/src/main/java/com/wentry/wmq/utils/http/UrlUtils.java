package com.wentry.wmq.utils.http;

import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.domain.registry.clients.ClientInfo;
import com.wentry.wmq.domain.registry.clients.ConsumerInstanceInfo;

/**
 * @Description:
 * @Author: tangwc
 */
public class UrlUtils {

    public static final String HTTP = "http://";
    public static final String PARTITIONS_SYNC_PRODUCER = "/wmq/producer/partitions/sync";
    public static final String PARTITIONS_SYNC_CONSUMER = "/wmq/consumer/partitions/sync";
    public static final String PARTITIONS_SYNC_BROKER = "/wmq/broker/partitions/sync";

    public static final String SEND_MSG_BROKER = "/wmq/broker/msg/write";
    public static final String PULL_MSG_BROKER = "/wmq/broker/msg/pull";
    public static final String REPLICA_SYNC_PUSH_BROKER = "/wmq/broker/replica/sync/push";//增量推送
    public static final String REPLICA_SYNC_PULL_BROKER = "/wmq/broker/replica/sync/pull";//全量拉取
    public static final String RE_BALANCE_CONSUMER = "/wmq/consumer/rebalance";

    public static String getPartitionsSyncUrl(ClientInfo info) {
        if (info.isProducer()) {
            return concat(info.getHost(), info.getPort()) + PARTITIONS_SYNC_PRODUCER;
        } else if (info.isConsumer()) {
            return concat(info.getHost(), info.getPort()) + PARTITIONS_SYNC_CONSUMER;
        }
        throw new RuntimeException("client type unexpect:" + info);
    }

    public static String getPartitionsSyncUrl(BrokerInfo info) {
        return concat(info.getHost(), info.getPort()) + PARTITIONS_SYNC_BROKER;
    }

    public static String getBrokerWriteMsg(BrokerInfo brokerInfo) {
        return concat(brokerInfo.getHost(), brokerInfo.getPort()) + SEND_MSG_BROKER;
    }

    public static String concat(String host, int port) {
        return HTTP + host + ":" + port;
    }

    public static String getReplicaSyncPullUrl(BrokerInfo info) {
        return concat(info.getHost(), info.getPort()) + REPLICA_SYNC_PULL_BROKER;
    }
    public static String getReplicaSyncPushUrl(BrokerInfo info) {
        return concat(info.getHost(), info.getPort()) + REPLICA_SYNC_PUSH_BROKER;
    }

    public static String getPullMsgUrl(BrokerInfo info) {
        return concat(info.getHost(), info.getPort()) + PULL_MSG_BROKER;
    }

    public static String getConsumerReBalanceUrl(ConsumerInstanceInfo info) {
        return concat(info.getHost(), info.getPort()) + RE_BALANCE_CONSUMER;
    }
}
