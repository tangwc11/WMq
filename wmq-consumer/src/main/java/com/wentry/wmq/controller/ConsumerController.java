package com.wentry.wmq.controller;

import com.wentry.wmq.domain.ConsumerInstance;
import com.wentry.wmq.domain.ConsumerState;
import com.wentry.wmq.transport.PartitionSyncReq;
import com.wentry.wmq.transport.PartitionSyncResp;
import com.wentry.wmq.transport.ReBalanceReq;
import com.wentry.wmq.transport.ReBalanceResp;
import com.wentry.wmq.utils.MixUtils;
import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
@RequestMapping("/wmq/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    @Autowired
    ConsumerState consumerState;

    @PostMapping("/partitions/sync")
    public PartitionSyncResp syncPartition(@RequestBody PartitionSyncReq partitionSyncReq) {

        log.info("fresh partitions received :{}", JsonUtils.toJson(partitionSyncReq));

        if (CollectionUtils.isEmpty(partitionSyncReq.getPartitions())) {
            return new PartitionSyncResp().setMsg("emp partitions");
        }

        consumerState.updatePartitions(partitionSyncReq);

        return new PartitionSyncResp().setMsg("ok");
    }


    @RequestMapping("/rebalance")
    public ReBalanceResp reBalance(@RequestBody ReBalanceReq req) {

        ConsumerInstance instance = consumerState.getConsumerInstanceMap().get(
                MixUtils.consumerInstanceKey(req.getTopic(), req.getPartition(), req.getGroup())
        );

        if (instance == null) {
            return new ReBalanceResp().setFailMsg("instance not found");
        }

        //停止实例
        instance.stop();
        log.info("instance :{} stopped...", JsonUtils.toJson(instance));

        return new ReBalanceResp().setSuccess(true).setLastOffset(instance.getLastAckOffset());
    }

}
