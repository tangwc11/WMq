package com.wentry.wmq.transport;

import java.io.Serializable;
import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReplicaSyncPullResp implements Serializable {

    private static final long serialVersionUID = 155889368018275L;

    boolean toEnd;
    List<byte[]> data;
    long endOffset;
    String failMsg;
    String extMsg;

    public String getExtMsg() {
        return extMsg;
    }

    public ReplicaSyncPullResp setExtMsg(String extMsg) {
        this.extMsg = extMsg;
        return this;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public ReplicaSyncPullResp setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }

    public boolean isToEnd() {
        return toEnd;
    }

    public ReplicaSyncPullResp setToEnd(boolean toEnd) {
        this.toEnd = toEnd;
        return this;
    }

    public List<byte[]> getData() {
        return data;
    }

    public ReplicaSyncPullResp setData(List<byte[]> data) {
        this.data = data;
        return this;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public ReplicaSyncPullResp setEndOffset(long endOffset) {
        this.endOffset = endOffset;
        return this;
    }
}
