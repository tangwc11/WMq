package com.wentry.wmq.utils.tmp;

import java.io.Serializable;

public class TestReqBody implements Serializable {

    private static final long serialVersionUID = 6739090103465621769L;
    String reqK1;
    String reqK2;

    public String getReqK1() {
        return reqK1;
    }

    public TestReqBody setReqK1(String reqK1) {
        this.reqK1 = reqK1;
        return this;
    }

    public String getReqK2() {
        return reqK2;
    }

    public TestReqBody setReqK2(String reqK2) {
        this.reqK2 = reqK2;
        return this;
    }
}