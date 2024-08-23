package com.wentry.wmq.config;

import com.wentry.wmq.domain.BrokerState;
import com.wentry.wmq.utils.file.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @Description:
 * @Author: tangwc
 */
@Configuration
public class BrokerConfiguration implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(BrokerConfiguration.class);

    @Value("${wmq.baseDir:wmq/db}")
    String baseDir;

    @Autowired
    BrokerState brokerState;

    @Override
    public void afterSingletonsInstantiated() {
        if (StringUtils.isNoneEmpty(baseDir)) {
            log.info("broker:{} db baseDir change from :{},to :{}", brokerState.getBrokerId(), FileUtils.DB_BASE_DIR, baseDir);
            FileUtils.DB_BASE_DIR = baseDir;
        }
    }
}
