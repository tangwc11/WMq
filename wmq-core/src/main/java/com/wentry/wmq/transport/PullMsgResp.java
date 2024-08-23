package com.wentry.wmq.transport;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 */
public class PullMsgResp implements Serializable {
    private static final long serialVersionUID = -2327525384889195801L;

    private String failMsg;
    private String extMsg;
    private long endOffset;
    private List<String> msgList;
    private boolean toEnd;

    public boolean needStop(){
        return toEnd || CollectionUtils.isEmpty(msgList) || StringUtils.isNotBlank(failMsg);
    }

    public String getExtMsg() {
        return extMsg;
    }

    public PullMsgResp setExtMsg(String extMsg) {
        this.extMsg = extMsg;
        return this;
    }

    public boolean isToEnd() {
        return toEnd;
    }

    public PullMsgResp setToEnd(boolean toEnd) {
        this.toEnd = toEnd;
        return this;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public PullMsgResp setFailMsg(String failMsg) {
        this.failMsg = failMsg;
        return this;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public PullMsgResp setEndOffset(long endOffset) {
        this.endOffset = endOffset;
        return this;
    }

    public List<String> getMsgList() {
        return msgList;
    }

    public PullMsgResp setMsgList(List<String> msgList) {
        this.msgList = msgList;
        return this;
    }

}
