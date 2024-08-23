package com.wentry.wmq.controller;

import com.wentry.wmq.utils.http.HttpUtils;
import com.wentry.wmq.utils.tmp.TestReqBody;
import com.wentry.wmq.utils.http.ReqBody;
import com.wentry.wmq.utils.tmp.RespBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * @Description:
 * @Author: tangwc
 */
@RestController
@RequestMapping("/wmq/internal")
public class TestController {

    private static final Logger log = LoggerFactory.getLogger(TestController.class);

    @PostMapping("/test")
    public RespBody test(@RequestBody ReqBody data) {
        return new RespBody()
                .setK1(data.getData("k1", String.class))
                .setReq(data.getData("body", TestReqBody.class));
    }

    @GetMapping("/testGet")
    public RespBody testGet(String k1,int k2) {
        log.info("testGet k1:{},k2:{}", k1, k2);
        return new RespBody().setK1(k1);
    }

    @RequestMapping("/testHttpClient")
    public RespBody test() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("k1", "hello");
        map.put("body", new TestReqBody().setReqK1("rek1").setReqK2("reK2"));
        return HttpUtils.post("http://localhost:10200/wmq/internal/test", map, RespBody.class);
    }

    @RequestMapping("/testHttpGet")
    public RespBody testHttpGet() {
        HashMap<String, String> map = new HashMap<>();
        map.put("k1", "hello");
        map.put("k2", "2");
        return HttpUtils.get("http://localhost:10200/wmq/internal/testGet", map, RespBody.class);
    }


}
