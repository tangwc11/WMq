package com.wentry.wmq.controller;

import com.wentry.wmq.domain.ProducerState;
import com.wentry.wmq.domain.filter.UniqFilter;
import com.wentry.wmq.domain.message.WMqMessage;
import com.wentry.wmq.domain.router.PartitionRouter;
import com.wentry.wmq.domain.registry.brokers.BrokerInfo;
import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.transport.PartitionSyncResp;
import com.wentry.wmq.transport.WriteMsgReq;
import com.wentry.wmq.transport.WriteRes;
import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.http.UrlUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @Description:
 * @Author: tangwc
 */
@RestController
@RequestMapping("/wmq/producer")
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class);

    @Autowired
    ProducerState producerState;

    /**
     * 接受partition分区同步消息
     */
    @PostMapping("/partitions/sync")
    public PartitionSyncResp syncPartition(@RequestBody PartitionSyncReq partitionSyncReq) {
        log.info("fresh partitions received :{}", JsonUtils.toJson(partitionSyncReq));

        if (CollectionUtils.isEmpty(partitionSyncReq.getPartitions())) {
            return new PartitionSyncResp().setMsg("emp partitions");
        }

        producerState.updatePartitions(partitionSyncReq);
        return new PartitionSyncResp().setMsg("ok");
    }

    @Autowired
    PartitionRouter partitionRouter;

    @Autowired
    UniqFilter uniqFilter;

    @GetMapping("/send")
    public String sendMq(String topic, String msg,
                         @RequestParam(required = false) String key,
                         @RequestParam(required = false) String uniq) {

        return doSend(topic, msg, key, uniq);
    }


    @GetMapping("/stressTest")
    public String stressTest(String topic, String msg,
                             @RequestParam(required = false) String key,
                             @RequestParam(required = false) String uniq,
                             @RequestParam(required = false,defaultValue = "0") int sleep,
                             long times) {

        long start = System.currentTimeMillis();
        for (long i = 0; i < times; i++) {
            String s = doSend(topic, msg + i, key, uniq);
            if (sleep>0) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
        long cost = System.currentTimeMillis() - start;
        StringBuilder res = new StringBuilder();
        res.append("send total:" + times + " msg:" + msg).append("\n");
        res.append("cost:" + (cost) + " ms").append("\n");
        res.append("average:" + (cost / times) + " ms").append("\n");

        // 使用BigDecimal来提高精度
        BigDecimal totalBD = new BigDecimal(times);
        BigDecimal costBD = new BigDecimal(cost);
        BigDecimal qps = totalBD.divide(costBD.divide(new BigDecimal(1_000)), 2, BigDecimal.ROUND_HALF_UP);

        res.append("qps:" + qps.toString());

        return res.toString();
    }

    private String doSend(String topic, String msg, String key, String uniq) {
        WMqMessage message = new WMqMessage().setTopic(topic)
                .setMsg(msg)
                .setKey(key)
                .setUniq(uniq);

        Map<Integer, BrokerInfo> partitionMap = producerState.getPartitions().get(topic);
        if (MapUtils.isEmpty(partitionMap)) {
            log.info("partition emp for topic:{}", topic);
            return "partitions emp for topic:" + topic;
        }

        Integer partition = partitionRouter.route(partitionMap.keySet(), message);
        if (partition == null) {
            return "partition route fail";
        }

        message.setPartition(partition);

        if (uniqFilter.filtered(message)) {
            return "filtered by uniqFilter";
        }

//        log.info("msg:{},route to partition:{}", JsonUtils.toJson(message), partition);

        BrokerInfo brokerInfo = partitionMap.get(partition);

        //请求
        return JsonUtils.toJson(HttpUtils.post(UrlUtils.getBrokerWriteMsg(brokerInfo),
                new WriteMsgReq().setMsg(message), WriteRes.class));
    }

}
