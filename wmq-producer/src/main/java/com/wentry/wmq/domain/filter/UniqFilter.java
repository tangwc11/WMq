package com.wentry.wmq.domain.filter;

import com.wentry.wmq.domain.message.WMqMessage;

/**
 * @Description:
 * @Author: tangwc
 */
public interface UniqFilter {


    /**
     * 自己实现过滤细节，可以选择是否区分partition，默认实现是区分的
     */
    boolean filtered(WMqMessage message);
}
