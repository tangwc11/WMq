package com.wentry.wmq.domain.storage;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.common.Closable;
import com.wentry.wmq.transport.ReadRes;
import com.wentry.wmq.transport.PullMsgReq;
import com.wentry.wmq.transport.PullMsgResp;
import com.wentry.wmq.transport.ReplicaSyncPullReq;
import com.wentry.wmq.transport.ReplicaSyncPullResp;
import com.wentry.wmq.utils.json.JsonUtils;
import com.wentry.wmq.utils.seriliaztion.SerializationUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class BrokerReader implements Closable {

    private static final Logger log = LoggerFactory.getLogger(BrokerReader.class);

    @Autowired
    BrokerState brokerState;

    private final Map<String, DbReader> dbReaderMap = new ConcurrentHashMap<>();

//    public PullMsgResp readTest(PullMsgReq req) {
//        String topic = req.getTopic();
//        int partition = req.getPartition();
//        String group = req.getGroup();
//
//
//        DbReader reader = dbReaderMap.computeIfAbsent(
//                dbReaderKey(topic, partition, group), s -> new DbReader(topic, partition)
//        );
//
//        ReadRes readRes;
//        int pullSize = req.getPullSize();
//        int currSize = 0;
//        long startOffset = req.getStartOffset();
//        long endOffset;
//        List<String> msgList = new ArrayList<>();
//        do {
//            readRes = reader.read(startOffset);
//            if (readRes.getData() != null) {
//                BaseMsg baseMsg = SerializationUtils.deserialize(readRes.getData(), BaseMsg.class);
//                if (baseMsg == null) {
//                    msgList.add("");
//                } else {
//                    msgList.add(baseMsg.getMsg());
//                }
//            }
//            currSize++;
//            startOffset = readRes.getNextOffset();
//            endOffset = readRes.getNextOffset();
//        } while (StringUtils.isEmpty(readRes.getFailMsg())
//                && !readRes.isToEnd()
//                && readRes.getData() != null
//                && currSize < pullSize);
//
//        return new PullMsgResp()
//                .setExtMsg(readRes.getExtMsg())
//                .setEndOffset(endOffset)
//                .setMsgList(msgList)
//                .setToEnd(readRes.isToEnd())
//                .setFailMsg(readRes.getFailMsg())
//                ;
//
//    }

    public PullMsgResp read(PullMsgReq req) {

        Set<Integer> partitions = brokerState.getAsLeaderTopicPartitions().get(req.getTopic());
        if (CollectionUtils.isEmpty(partitions)) {
            return new PullMsgResp().setFailMsg("not topic leader");
        }

        if (!partitions.contains(req.getPartition())) {
            return new PullMsgResp().setFailMsg("not partition leader");
        }

        /**
         * 每个partition，一个dbReader
         */
        String topic = req.getTopic();
        int partition = req.getPartition();
        String group = req.getGroup();


        DbReader reader = dbReaderMap.computeIfAbsent(
                dbReaderKey(topic, partition, group), s -> new DbReader(topic, partition)
        );

        ReadRes readRes;
        int pullSize = req.getPullSize();
        int currSize = 0;
        long startOffset = req.getStartOffset();
        long endOffset;
        List<String> msgList = new ArrayList<>();
        do {
            readRes = reader.read(startOffset);
            if (readRes.getData() != null) {
                BaseMsg baseMsg = SerializationUtils.deserialize(readRes.getData(), BaseMsg.class);
                if (baseMsg == null) {
                    msgList.add("");
                } else {
                    msgList.add(baseMsg.getMsg());
                }
            }
            currSize++;
            startOffset = readRes.getNextOffset();
            endOffset = readRes.getNextOffset();
        } while (StringUtils.isEmpty(readRes.getFailMsg())
                && !readRes.isToEnd()
                && readRes.getData() != null
                && currSize < pullSize);

        return new PullMsgResp().setExtMsg(readRes.getExtMsg()).setEndOffset(endOffset).setMsgList(msgList)
                .setToEnd(readRes.isToEnd()).setFailMsg(readRes.getFailMsg());
    }

    private String dbReaderKey(String topic, int partition, String group) {
        return String.join(":", topic, String.valueOf(partition), group);
    }

    @Override
    public void close() {
        for (DbReader value : dbReaderMap.values()) {
            value.close();
        }
    }


//    public static void main(String[] args) throws InterruptedException {
////        test1();
//        test2();
//    }

//    private static void test1() {
//        BrokerReader brokerReader = new BrokerReader();
//        PullMsgResp hello;
//        long endOffset = 0;
//        int total = 0;
//        long start = System.nanoTime();
//        do {
//            hello = brokerReader.readTest(new PullMsgReq().setTopic("hello").setPartition(2).setPullSize(10).setStartOffset(endOffset));
//            endOffset = hello.getEndOffset();
//            total += hello.getMsgList().size();
//            System.out.println(JsonUtils.toJson(hello));
//        } while (!hello.needStop());
//
//        long cost = System.nanoTime() - start;
//        System.out.println("read total:" + total + " msg ");
//        System.out.println("cost:" + (cost) + " ns");
//        System.out.println("average:" + (cost / total) + " ns");
//
//        // 使用BigDecimal来提高精度
//        BigDecimal totalBD = new BigDecimal(total);
//        BigDecimal costBD = new BigDecimal(cost);
//        BigDecimal qps = totalBD.divide(costBD.divide(new BigDecimal(1_000_000_000)), 2, BigDecimal.ROUND_HALF_UP);
//
//        System.out.println("qps:" + qps.toString());
//    }

//    private static void test2() throws InterruptedException {
//        BrokerReader brokerReader = new BrokerReader();
//        PullMsgResp hello;
//        long endOffset = 0;
//        int total = 0;
//        do {
//            hello = brokerReader.readTest(new PullMsgReq().setTopic("hello").setPartition(2).setPullSize(10).setStartOffset(endOffset));
//            endOffset = hello.getEndOffset();
//            total += hello.getMsgList().size();
//            System.out.println(JsonUtils.toJson(hello));
//            if (hello == null || hello.needStop()) {
//                Thread.sleep(1000);
//            }
//        } while (true);
//
//    }

    public ReplicaSyncPullResp readForSync(ReplicaSyncPullReq req) {

        Set<Integer> partitions = brokerState.getAsLeaderTopicPartitions().get(req.getTopic());
        if (CollectionUtils.isEmpty(partitions)) {
            return new ReplicaSyncPullResp().setFailMsg("not topic leader");
        }

        if (!partitions.contains(req.getPartition())) {
            return new ReplicaSyncPullResp().setFailMsg("not partition leader");
        }

        /**
         * 每个partition，一个dbReader
         */
        String topic = req.getTopic();
        int partition = req.getPartition();

        DbReader reader = dbReaderMap.computeIfAbsent(
                dbReaderKey(topic, partition, req.getPullBrokerGroup()),
                s -> new DbReader(topic, partition)
        );

        ReadRes readRes;
        int pullSize = req.getPullSize();
        int currSize = 0;
        long startOffset = req.getOffset();
        long endOffset;
        List<byte[]> datas = new ArrayList<>();
        do {
            readRes = reader.read(startOffset);
            if (readRes.getData() != null) {
                byte[] data = readRes.getData();
                if (data != null) {
                    datas.add(data);
                }
            }
            log.info("readForSync topic:{},partitionL{},currSize:{},req:{},res:{}",
                    topic, partition, currSize, JsonUtils.toJson(req), JsonUtils.toJson(readRes));
            currSize++;
            startOffset = readRes.getNextOffset();
            endOffset = readRes.getNextOffset();
        } while (StringUtils.isEmpty(readRes.getFailMsg())
                && !readRes.isToEnd()
                && readRes.getData() != null
                && currSize < pullSize);

        return new ReplicaSyncPullResp().setExtMsg(readRes.getExtMsg()).setEndOffset(endOffset)
                .setData(datas).setToEnd(readRes.isToEnd()).setFailMsg(readRes.getFailMsg());
    }
}
