package com.wentry.wmq.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class WMqThreadFactory implements ThreadFactory {
    private String threadNamePrefix = null;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final boolean daemon;
    private final int priority;

    private Supplier<String> nameConsumer;

    public WMqThreadFactory(Supplier<String> nameConsumer, boolean daemon, int priority) {
        this.daemon = daemon;
        this.priority = priority;
        this.nameConsumer = nameConsumer;
    }

    public WMqThreadFactory(String threadNamePrefix, boolean daemon, int priority) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t;
        if (nameConsumer != null) {
            t = new Thread(r, nameConsumer.get() + threadNumber.getAndIncrement());
        } else {
            t = new Thread(r, threadNamePrefix + threadNumber.getAndIncrement());
        }
        t.setDaemon(daemon);
        t.setPriority(priority);
        return t;
    }

}