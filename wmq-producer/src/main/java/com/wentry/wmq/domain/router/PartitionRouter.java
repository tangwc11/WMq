package com.wentry.wmq.domain.router;

import com.wentry.wmq.domain.message.WMqMessage;

import java.util.Set;

/**
 * @Description: 指定分区，可以按key规则分区
 * @Author: tangwc
 */
public interface PartitionRouter {


    Integer route(Set<Integer> partitions, WMqMessage wMqMessage);

}
