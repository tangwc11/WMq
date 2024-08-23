package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class PartitionSyncResp implements Serializable {
    private static final long serialVersionUID = 1834129532095876562L;

    private String msg;

    public String getMsg() {
        return msg;
    }

    public PartitionSyncResp setMsg(String msg) {
        this.msg = msg;
        return this;
    }
}
