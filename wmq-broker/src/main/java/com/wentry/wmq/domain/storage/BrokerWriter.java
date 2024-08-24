package com.wentry.wmq.domain.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wentry.wmq.common.Closable;
import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.domain.isr.IsrSet;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.transport.ReplicaSyncPushReq;
import com.wentry.wmq.transport.ReplicaSyncPushResp;
import com.wentry.wmq.transport.WriteMsgReq;
import com.wentry.wmq.transport.WriteRes;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.seriliaztion.SerializationUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class BrokerWriter implements Closable, SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(BrokerWriter.class);

    @Autowired
    BrokerState brokerState;

    private final Map<String, DbWriter> dbWriterMap = new ConcurrentHashMap<>();

    //带preOffset检查的write，用作同步副本
    public WriteRes write(String topic, int partition) throws FileNotFoundException {
        DbWriter dbWriter = getDbWriter(topic, partition);

        return syncAllDataFromLeader(topic, partition, dbWriter);
    }

    private WriteRes syncAllDataFromLeader(String topic, int partition, DbWriter dbWriter) {
        //异步的，里面有专门的一个线程处理
        dbWriter.syncAll(topic, partition, brokerState);
        return new WriteRes();
    }

    public WriteRes write(String topic, byte[] bytes, int partition) throws FileNotFoundException {
        DbWriter dbWriter = getDbWriter(topic, partition);
        return dbWriter.write(bytes);
    }

    private DbWriter getDbWriter(String topic, int partition) throws FileNotFoundException {
        String key = key(topic, partition);
        DbWriter dbWriter = dbWriterMap.get(key);
        if (dbWriter != null) {
            return dbWriter;
        }
        dbWriter = new DbWriter(topic, partition);
        dbWriterMap.put(key, dbWriter);
        return dbWriter;
    }

    private String key(String topic, int partition) {
        return topic + ":" + partition;
    }


    @Override
    public void close() {
        for (Map.Entry<String, DbWriter> dbWriter : dbWriterMap.entrySet()) {
            dbWriter.getValue().close();
        }
    }

    public WriteRes write(WriteMsgReq req) {
        /**
         * 一条消息如何流转：
         * 1. producer生产消息，指定key或者按照一定算法划分到partition
         * 2. producer拉取的broker信息，找到partition对应是哪个broker，并请求broker写入
         * 3. broker写入成功之后，返回，并通知follower进行同步写入
         * 4. 更新ISR信息，并广播给所有的follower
         */
        Set<Integer> partitions = brokerState.getAsLeaderTopicPartitions().get(req.getMsg().getTopic());
        if (CollectionUtils.isEmpty(partitions)) {
            return new WriteRes().setFailMsg("none partitions");
        }

        if (!partitions.contains(req.getMsg().getPartition())) {
            return new WriteRes().setFailMsg("partition not including, the partitions is:" + partitions);
        }
        try {
            BaseMsg baseMsg = new BaseMsg().setMsg(req.getMsg().getMsg());
            byte[] bytes = SerializationUtils.serialize(baseMsg.setKey(req.getMsg().getKey()).setT(req.getMsg().getCreateTime()));
            WriteRes writeRes = write(req.getMsg().getTopic(), bytes, req.getMsg().getPartition());
            log.info("writeRes:{}, msgReq:{}", writeRes, req);
            /**
             * follower接收到的信息有
             * 1。 上次写入的offset，此次写入的offset
             * 2。 此次写入的消息体
             */
            //主动通知follower进行同步
            asyncBroadCastReplica(req, writeRes);

            return writeRes;
        } catch (Exception e) {
            e.printStackTrace();
            return new WriteRes().setFailMsg(e.getMessage());
        }

    }

    private final ThreadPoolExecutor syncThread = new ThreadPoolExecutor(1,
            1,
            1,
            TimeUnit.HOURS,
            new LinkedBlockingDeque<>(10),
            new ThreadFactoryBuilder()
                    .setNameFormat("sync-replica-thread-")
                    .setDaemon(true)
                    .build(),
            new ThreadPoolExecutor.CallerRunsPolicy());


    Map<String, AtomicInteger> topicCount = new ConcurrentHashMap<>();

    private void asyncBroadCastReplica(WriteMsgReq req, WriteRes writeRes) {

        syncThread.execute(new Runnable() {
            @Override
            public void run() {
                Map<String, Map<Integer, IsrSet>> isr = brokerState.getIsr();

                AtomicInteger count = topicCount.get(req.getMsg().getTopic());
                if (count==null) {
                    count = new AtomicInteger();
                    topicCount.put(req.getMsg().getTopic(), count);
                }
                if (count.incrementAndGet() < 100) {
                    return;
                }
                //通知follower同步，并重置计数器
                count.set(0);

                doBroadCastSyncAll();
            }

            private void doBroadCastSyncAll() {
                Map<Integer, Set<String>> partitionFollowers = brokerState.getTopicPartitionsFollowers()
                        .get(req.getMsg().getTopic());
                if (MapUtils.isEmpty(partitionFollowers)) {
                    return;
                }
                Set<String> followers = partitionFollowers.get(req.getMsg().getPartition());
                IsrSet isrSet = new IsrSet().setOffset(writeRes.getLatestOffset());
                ReplicaSyncPushReq replicaSyncPushReq = new ReplicaSyncPushReq()
                        .setTopic(req.getMsg().getTopic())
                        .setPartition(req.getMsg().getPartition())
                        .setPreOffset(writeRes.getPreOffset());
                for (String follower : followers) {
                    BrokerInfo brokerInfo = brokerState.getBrokerInfoMap().get(follower);
                    if (brokerInfo == null) {
                        continue;
                    }
                    ReplicaSyncPushResp res = HttpUtils.post(
                            UrlUtils.getReplicaSyncPushUrl(brokerInfo), replicaSyncPushReq, ReplicaSyncPushResp.class
                    );
                    if (res != null && res.isSuccess()) {
                        isrSet.getIsr().add(follower);
                    }
                }
                //todo wch ISR机制完善
                if (CollectionUtils.isEmpty(isrSet.getIsr())) {
                    //没有成功的，不用更新
                    return;
                }
                //把老的顶掉
//                isr.get(req.getMsg().getTopic()).put(req.getMsg().getPartition(), isrSet);
                //周知isr的任务，有另外的异步线程定时处理，见：ScheduleBroadCastISR
            }
        });

    }

    @Override
    public void afterSingletonsInstantiated() {

        //开启主动同步的工作
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Map<String, Set<Integer>> asFollowerTopicPartitions = brokerState.getAsFollowerTopicPartitions();
                for (Map.Entry<String, Set<Integer>> ety : asFollowerTopicPartitions.entrySet()) {
                    String topic = ety.getKey();
                    Set<Integer> partitions = ety.getValue();
                    for (Integer partition : partitions) {
                        try {
                            DbWriter dbWriter = getDbWriter(topic, partition);
                            syncAllDataFromLeader(topic, partition, dbWriter);
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        }, 0, 5, TimeUnit.SECONDS);

    }
}
