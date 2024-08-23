package com.wentry.wmq.config;

import com.wentry.wmq.common.Closable;
import com.wentry.wmq.domain.registry.zookeeper.ZkRegistry;
import com.wentry.wmq.domain.registry.zookeeper.ZookeeperRegistryImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
@Configuration
public class WMqAutoConfiguration implements  ApplicationContextAware, SmartInitializingSingleton {

    @Autowired
    WMqConfig wMqConfig;

    @Bean
    @ConditionalOnMissingBean(ZkRegistry.class)
    public ZkRegistry registry(){
        return new ZookeeperRegistryImpl(wMqConfig);
    }

    private ApplicationContext ctx;


    List<Map.Entry<String, Closable>> sortClosable;
    @PreDestroy
    public void end() {
        for (Map.Entry<String, Closable> ety : sortClosable) {
            ety.getValue().close();
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.ctx = applicationContext;
    }

    Map<String, Closable> beans = new ConcurrentHashMap<>();
    @Override
    public void afterSingletonsInstantiated() {
        sortClosable = ctx.getBeansOfType(Closable.class).entrySet()
                        .stream()
                        .sorted(Comparator.comparingInt(o -> o.getValue().order())).collect(Collectors.toList());
        System.out.println("done");
    }
}
