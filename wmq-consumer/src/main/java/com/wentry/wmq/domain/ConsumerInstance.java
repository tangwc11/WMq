package com.wentry.wmq.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wentry.wmq.openapi.WmqListener;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.transport.PullMsgReq;
import com.wentry.wmq.transport.PullMsgResp;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.zk.ZkPaths;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: tangwc
 */
public class ConsumerInstance {

    private static final Logger log = LoggerFactory.getLogger(ConsumerInstance.class);

    @JsonIgnore
    private ConsumerState consumerState;
    private WmqListener listener;
    private int partition;
    private String topic;
    private long lastAckOffset = 0L;
    private volatile boolean started = false;
    private ScheduledExecutorService scheduledExecutorService;
    ThreadPoolExecutor threadPoolExecutor;
    Thread consumerThread;

    public ConsumerInstance(ConsumerState consumerState, WmqListener listener, int partition, String topic) {
        this.consumerState = consumerState;
        this.listener = listener;
        this.partition = partition;
        this.topic = topic;
    }

    public ConsumerInstance start() {
        if (started) {
            return this;
        }
        this.started = true;

        this.consumerThread = new Thread(() -> {
            try {
                startConsumer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        this.consumerThread.setName("consumer-|" + topic + "|-" + partition);
        this.consumerThread.start();

        scheduledExecutorService = Executors.newScheduledThreadPool(1);

        //5s一次上报offset
        scheduledExecutorService.scheduleAtFixedRate(this::reportAckOffset, 5, 5, TimeUnit.SECONDS);

//        关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop(true)));

        return this;
    }

    private void reportAckOffset() {
        try {
            consumerState.reportConsumerOffset(topic, listener.consumerGroup(), partition, getLastAckOffset());
        } catch (Exception e) {
            log.error("ConsumerInstance.reportAckOffset()");
        }
    }

    private void startConsumer() throws InterruptedException {
        long start = getLastAckOffset();
        while (started) {
            BrokerInfo brokerInfo = consumerState.getPartitions().get(topic).get(partition);
            if (brokerInfo == null) {
                log.info("brokerInfo null for topic:{},partition:{}", topic, partition);
                Thread.sleep(sleepTime);
                incrSleepTime();
                continue;
            }
            start = doConsume(start, brokerInfo);
        }
    }


    //渐进式休眠
    long sleepTime = 10000;

    private long doConsume(long startOffset, BrokerInfo brokerInfo) {
        try {
            PullMsgReq pullMsgReq = new PullMsgReq().setTopic(topic).setGroup(listener.consumerGroup())
                    .setPartition(partition).setPullSize(50).setStartOffset(startOffset);
            PullMsgResp res = HttpUtils.post(UrlUtils.getPullMsgUrl(brokerInfo), pullMsgReq, PullMsgResp.class);

            if (res == null || CollectionUtils.isEmpty(res.getMsgList()) || StringUtils.isNotBlank(res.getFailMsg())) {
                Thread.sleep(sleepTime);
                incrSleepTime();
                if (res != null && StringUtils.isNotBlank(res.getFailMsg())) {
                    log.info("fetch msg, topic:{},group:{},partition:{} res:{}", topic, listener.consumerGroup(), partition, JsonUtils.toJson(res));
                }
                //继续从原offset开始消费
                return startOffset;
            }

            resetSleepTime();
            for (String msg : res.getMsgList()) {
                try {
                    //这里可以支持 orderly 或者 concurrent消费，即单线程或者多线程
                    listener.onMessage(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            setLastAckOffset(res.getEndOffset());
            return res.getEndOffset();
        } catch (Exception e) {
            log.error("ConsumerInstance.doConsume(" + "startOffset = " + startOffset + ", brokerInfo = " + brokerInfo + ")", e);
        }
        return startOffset;
    }

    private void resetSleepTime() {
        sleepTime = 1000;
    }

    private void incrSleepTime() {
        sleepTime = Math.min(10000, sleepTime + 1000);
    }

    public ConsumerState getConsumerState() {
        return consumerState;
    }

    public ConsumerInstance setConsumerState(ConsumerState consumerState) {
        this.consumerState = consumerState;
        return this;
    }

    public WmqListener getListener() {
        return listener;
    }

    public ConsumerInstance setListener(WmqListener listener) {
        this.listener = listener;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public ConsumerInstance setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public ConsumerInstance setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public long getLastAckOffset() {
        return lastAckOffset;
    }

    public ConsumerInstance setLastAckOffset(long lastAckOffset) {
        this.lastAckOffset = lastAckOffset;
        return this;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean stop(boolean deleteZkPath) {
        log.info("stopping triggered, deleteZkPath:{}", deleteZkPath);
        if (!this.started) {
            return false;
        }
        this.started = false;
        if (this.consumerThread != null) {
            this.consumerThread.interrupt();
            this.consumerThread.stop();
        }
        if (this.threadPoolExecutor != null) {
            this.threadPoolExecutor.shutdownNow();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        reportAckOffset();

        if (deleteZkPath) {
            //把自身的节点删除
            consumerState.registry.deletePath(ZkPaths.getConsumerInstanceNode(
                    consumerState.getClusterName(),
                    getTopic(),
                    getListener().consumerGroup(),
                    getPartition()));
        }
        return true;
    }


    public static void main(String[] args) throws InterruptedException {

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                1,
                1,
                TimeUnit.HOURS,
                new LinkedBlockingDeque<>(10),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .build(),
                new ThreadPoolExecutor.CallerRunsPolicy());


        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        System.out.println("sleeping....");
                        Thread.sleep(5000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        threadPoolExecutor.shutdownNow();


        Thread.sleep(1000000);

    }

}
