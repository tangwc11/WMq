package com.wentry.wmq.transport;

import java.io.Serializable;

/**
 * @Description:
 * @Author: tangwc
 */
public class WriteRes implements Serializable {

    private static final long serialVersionUID = 1071815934182126445L;

    private String failMsg;
    private long preOffset;
    private long latestOffset;

    public String getFailMsg() {
        return failMsg;
    }

    public WriteRes setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }

    public long getPreOffset() {
        return preOffset;
    }

    public WriteRes setPreOffset(long preOffset) {
        this.preOffset = preOffset;
        return this;
    }

    public long getLatestOffset() {
        return latestOffset;
    }

    public WriteRes setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
        return this;
    }

    @Override
    public String toString() {
        return "WriteRes{" +
                "preOffset=" + preOffset +
                ", latestOffset=" + latestOffset +
                '}';
    }
}
