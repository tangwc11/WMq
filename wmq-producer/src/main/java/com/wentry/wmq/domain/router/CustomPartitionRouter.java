package com.wentry.wmq.domain.router;

import com.wentry.wmq.domain.message.WMqMessage;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class CustomPartitionRouter implements PartitionRouter{

    private final PartitionRouter base = new DefaultPartitionRouter();

    @Override
    public Integer route(Set<Integer> partitions, WMqMessage wMqMessage) {
        if (StringUtils.equals("1", wMqMessage.getKey()) && partitions.contains(0)) {
            return 0;
        }
        return base.route(partitions, wMqMessage);
    }
}
