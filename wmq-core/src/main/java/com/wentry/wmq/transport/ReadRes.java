package com.wentry.wmq.transport;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReadRes {

    private byte[] data;
    private long nextOffset;
    private String failMsg;
    private String extMsg;
    private boolean toEnd;

    public String getExtMsg() {
        return extMsg;
    }

    public ReadRes setExtMsg(String extMsg) {
        this.extMsg = extMsg;
        return this;
    }

    public boolean isToEnd() {
        return toEnd;
    }

    public ReadRes setToEnd(boolean toEnd) {
        this.toEnd = toEnd;
        return this;
    }

    public byte[] getData() {
        return data;
    }

    public ReadRes setData(byte[] data) {
        this.data = data;
        return this;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public ReadRes setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
        return this;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public ReadRes setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }
}
