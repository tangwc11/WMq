package com.wentry.wmq.domain.router;

import com.wentry.wmq.domain.message.WMqMessage;
import com.wentry.wmq.utils.MixUtils;

import java.util.Set;

/**
 * @Description:
 * @Author: tangwc
 */
public class DefaultPartitionRouter implements PartitionRouter{
    @Override
    public Integer route(Set<Integer> partitions, WMqMessage wMqMessage) {
        return MixUtils.randomGet(partitions);
    }

}
