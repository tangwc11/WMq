package com.wentry.wmq.utils.tmp;

public class RespBody {

    TestReqBody req;
    String k1;

    public TestReqBody getReq() {
        return req;
    }

    public RespBody setReq(TestReqBody req) {
        this.req = req;
        return this;
    }

    public String getK1() {
        return k1;
    }

    public RespBody setK1(String k1) {
        this.k1 = k1;
        return this;
    }
}