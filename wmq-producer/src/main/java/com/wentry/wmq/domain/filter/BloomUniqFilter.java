package com.wentry.wmq.domain.filter;

import com.wentry.wmq.domain.message.WMqMessage;

/**
 * @Description:
 * @Author: tangwc
 */
public class BloomUniqFilter implements UniqFilter{
    @Override
    public boolean filtered(WMqMessage message) {
        return false;
    }
}
