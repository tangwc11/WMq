package com.wentry.wmq;

import com.wentry.wmq.config.WMqConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @Description:
 * @Author: tangwc
 */
@SpringBootApplication
public class BrokerApplication {

    private static final Logger log = LoggerFactory.getLogger(BrokerApplication.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(BrokerApplication.class);
        log.info("broker start success with config :{}", ctx.getBean(WMqConfig.class));
    }

}
