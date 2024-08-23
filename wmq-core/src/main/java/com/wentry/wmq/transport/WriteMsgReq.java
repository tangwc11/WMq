package com.wentry.wmq.transport;

import com.wentry.wmq.domain.message.WMqMessage;

/**
 * @Description:
 * @Author: tangwc
 */
public class WriteMsgReq extends BaseReq {
    private static final long serialVersionUID = -9021787583417704008L;

    private WMqMessage msg;

    public WMqMessage getMsg() {
        return msg;
    }

    public WriteMsgReq setMsg(WMqMessage msg) {
        this.msg = msg;
        return this;
    }

    @Override
    public String toString() {
        return "WriteMsgReq{" +
                "msg=" + msg +
                '}';
    }
}
