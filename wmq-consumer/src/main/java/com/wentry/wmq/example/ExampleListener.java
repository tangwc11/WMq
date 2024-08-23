package com.wentry.wmq.example;

import com.wentry.wmq.openapi.WmqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class ExampleListener implements WmqListener {

    private static final Logger log = LoggerFactory.getLogger(ExampleListener.class);

    @Override
    public String topic() {
        return "hello";
    }

    @Override
    public String consumerGroup() {
        return "group-1";
    }

    @Override
    public void onMessage(String msg) {
        log.info("开始消费:{}", msg);
    }
}
