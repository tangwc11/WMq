package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReplicaSyncPushResp implements Serializable {
    private static final long serialVersionUID = -6644845885169026186L;

    private String failMsg;
    private boolean success;

    public String getFailMsg() {
        return failMsg;
    }

    public ReplicaSyncPushResp setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public ReplicaSyncPushResp setSuccess(boolean success) {
        this.success = success;
        return this;
    }
}
