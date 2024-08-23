package com.wentry.wmq.domain.storage;

import java.io.Serializable;

/**
 * @Description: 存入磁盘的信息，尽量减少字段冗余，topic不用、partition不用
 * @Author: tangwc
 */
public class BaseMsg implements Serializable {
    private static final long serialVersionUID = 2531533877413683060L;

    private long t;
    private String key;
    private String msg;

    public String getKey() {
        return key;
    }

    public BaseMsg setKey(String key) {
        this.key = key;
        return this;
    }

    public long getT() {
        return t;
    }

    public BaseMsg setT(long t) {
        this.t = t;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public BaseMsg setMsg(String msg) {
        this.msg = msg;
        return this;
    }
}
