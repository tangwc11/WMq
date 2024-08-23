package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReBalanceResp implements Serializable {
    private static final long serialVersionUID = -8293862496409720324L;

    private String failMsg;
    private boolean success;
    private long lastOffset;

    public String getFailMsg() {
        return failMsg;
    }

    public ReBalanceResp setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public ReBalanceResp setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public ReBalanceResp setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    @Override
    public String toString() {
        return "ReBalanceResp{" +
                "failMsg='" + failMsg + '\'' +
                ", success=" + success +
                ", lastOffset=" + lastOffset +
                '}';
    }
}
