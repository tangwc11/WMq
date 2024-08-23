package com.wentry.wmq.config;

import com.wentry.wmq.domain.filter.DefaultUniqFilter;
import com.wentry.wmq.domain.filter.UniqFilter;
import com.wentry.wmq.domain.router.DefaultPartitionRouter;
import com.wentry.wmq.domain.router.PartitionRouter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Description:
 * @Author: tangwc
 */
@Configuration
public class ProducerConfiguration {

    @Bean
    @ConditionalOnMissingBean(PartitionRouter.class)
    public PartitionRouter partitionRouter(){
        return new DefaultPartitionRouter();
    }

    @Bean
    @ConditionalOnMissingBean(UniqFilter.class)
    public UniqFilter uniqFilter(){
        //10s防重
        return new DefaultUniqFilter(10L);
    }
}
