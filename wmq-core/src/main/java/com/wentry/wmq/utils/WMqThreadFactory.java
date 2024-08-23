package com.wentry.wmq.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class WMqThreadFactory implements ThreadFactory {
    private final String threadNamePrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final boolean daemon;
    private final int priority;

    public WMqThreadFactory(String threadNamePrefix, boolean daemon, int priority) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, threadNamePrefix + threadNumber.getAndIncrement());
        t.setDaemon(daemon);
        t.setPriority(priority);
        return t;
    }

}