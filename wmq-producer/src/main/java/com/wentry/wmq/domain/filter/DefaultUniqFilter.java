package com.wentry.wmq.domain.filter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.wentry.wmq.domain.message.WMqMessage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: tangwc
 * 简单缓存实现的一定时间窗口保证消息唯一性
 */
public class DefaultUniqFilter implements UniqFilter {

    private static final Logger log = LoggerFactory.getLogger(DefaultUniqFilter.class);

    private final long windowSecond;

    public DefaultUniqFilter(long windowSecond) {
        this.windowSecond = windowSecond;
    }

    private final Map<String, Cache<String, String>> recordMap = new ConcurrentHashMap<>();

    @Override
    public boolean filtered(WMqMessage message) {
        if (message == null) {
            return true;
        }
        String uniq = message.getUniq();
        int partition = message.getPartition();
        String topic = message.getTopic();
        try {
            if (uniq == null) {
                return false;
            }
            /**
             * 按topic和partition划分即可，并且是broker内存缓存
             */
            String recordKey = recordKey(topic, partition);
            Cache<String, String> cache = recordMap.get(recordKey);

            /**
             * 使用guava实现
             * 1。以10秒为例
             * 2。缓存记录的是10秒内第一次访问msgId，缓存的是有效期是10秒，且是expireAfterWrite
             * 3。有缓存且不等于当次访问msgId，则被拦截
             * 4。否则放行
             */
            if (cache == null) {
                cache = CacheBuilder.newBuilder()
                        .expireAfterAccess(windowSecond, TimeUnit.SECONDS)
                        .build();
                recordMap.put(recordKey, cache);
            }

            String firstMsgId = cache.get(uniq, message::getMsgId);
            return !StringUtils.equals(firstMsgId, message.getMsgId());
        } catch (ExecutionException e) {
            log.error("DefaultUniqFilter.filtered(" + "topic = " + topic + ", partition = " + partition + ", uniq = " + uniq + ")", e);
        }
        return false;
    }

    private String recordKey(String topic, int partition) {
        return String.join("|", topic, String.valueOf(partition));
    }
}
