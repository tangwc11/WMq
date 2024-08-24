package com.wentry.wmq.controller;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.transport.PartitionSyncResp;
import com.wentry.wmq.transport.ReplicaSyncPullReq;
import com.wentry.wmq.transport.ReplicaSyncPullResp;
import com.wentry.wmq.transport.ReplicaSyncPushResp;
import com.wentry.wmq.transport.WriteRes;
import com.wentry.wmq.domain.storage.BrokerReader;
import com.wentry.wmq.domain.storage.BrokerWriter;
import com.wentry.wmq.transport.PullMsgReq;
import com.wentry.wmq.transport.PullMsgResp;
import com.wentry.wmq.transport.ReplicaSyncPushReq;
import com.wentry.wmq.transport.WriteMsgReq;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description:
 * @Author: tangwc
 */
@RestController
@RequestMapping("/wmq/broker")
public class BrokerController implements SmartInitializingSingleton {
    private static final Logger log = LoggerFactory.getLogger(BrokerController.class);

    @Autowired
    BrokerState brokerState;

    @Autowired
    BrokerWriter brokerWriter;

    @Autowired
    BrokerReader brokerReader;


    @PostMapping("/partitions/sync")
    public PartitionSyncResp syncPartition(@RequestBody PartitionSyncReq partitionSyncReq) {
        log.info("fresh partitions received :{}", JsonUtils.toJson(partitionSyncReq));

        if (CollectionUtils.isEmpty(partitionSyncReq.getPartitions())) {
            return new PartitionSyncResp().setMsg("emp partitions");
        }

        brokerState.updateLocalPartitions(partitionSyncReq);
        return new PartitionSyncResp().setMsg("ok");
    }

    @PostMapping("/replica/sync/push")
    public ReplicaSyncPushResp replicaSyncPush(@RequestBody ReplicaSyncPushReq req) {
        try {
            WriteRes res = brokerWriter.write(
                    req.getTopic(), req.getPartition()
            );
            if (StringUtils.isNoneEmpty(res.getFailMsg())) {
                return new ReplicaSyncPushResp().setFailMsg(res.getFailMsg());
            }
            return new ReplicaSyncPushResp().setSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
            return new ReplicaSyncPushResp().setFailMsg(e.getMessage());
        }
    }



    @PostMapping("/replica/sync/pull")
    public ReplicaSyncPullResp replicaSyncPull(@RequestBody ReplicaSyncPullReq req) {
        return brokerReader.readForSync(req);
    }

    /**
     * 如何写消息？
     *
     * 每个broker，负责的有topic-partition
     * 因此会有一个磁盘目录
     * ./wmq
     * .  |----db
     * .       |----topicA
     * .              |----partition-1
     * .                       |----file1
     * .                       |----file2
     * .                       |----...
     * .              |----partition-2
     * .                       |----
     * .       |----topicB
     * .              |----partition-1
     * .              |----partition-2
     * .
     * 目录结构如上，最后需要注意的是file文件，是最终存储日志的地方
     * kafka的rocketmq的设计是：
     * 1. 通过offset定位消息，再通过消息长度读取下一个消息，一个file的大小为1GB，超过的会写入到下一个消息
     * 2. 第二个文件的文件名为第一个文件的最后一个消息的offset
     * 3. 这样的话，通过一个offset就能定位到是哪一个数据文件
     * 4. 每个消息的长度不是固定的，消息格式简单设计为
     *   - 4个字节：32位，表示消息长度，最长为2^32 bytes = 4 GB，一条消息最大可以为4GB，序列化方式采用protoBuff，io采用NIO
     *   - length字节的数据
     * @return
     */
    @RequestMapping("/msg/write")
    public WriteRes writeMsg(@RequestBody WriteMsgReq req) {
        return brokerWriter.write(req);
    }

    @RequestMapping("/msg/pull")
    public PullMsgResp pullMsg(@RequestBody PullMsgReq req) {
        if (req.getPullSize()<=0) {
            return new PullMsgResp().setFailMsg("pull size <=0");
        }
        return brokerReader.read(req);
    }


    public static void main(String[] args) {
        BrokerInfo brokerInfo = new BrokerInfo().setHost("localhost").setPort(10200);

        PullMsgResp res;
        long start = 0;
        do {
            res = HttpUtils.post(UrlUtils.getPullMsgUrl(brokerInfo), new PullMsgReq()
                            .setTopic("hello")
                            .setGroup("group")
                            .setPartition(2)
                            .setPullSize(10)
                            .setStartOffset(start)
                    , PullMsgResp.class);
            if (res != null) {
                start = res.getEndOffset();
            }
            System.out.println(JsonUtils.toJson(res));
        } while (res != null && !res.isToEnd());
    }

    @Override
    public void afterSingletonsInstantiated() {
    }
}
