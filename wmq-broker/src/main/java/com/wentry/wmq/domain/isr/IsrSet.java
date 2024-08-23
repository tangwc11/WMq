package com.wentry.wmq.domain.isr;

import java.util.HashSet;
import java.util.Set;

/**
 * @Description:
 * ISR的几个设计要点：
 * 0。 首先说说ISR的作用，就是在leader崩溃之后，快速选主，ISR可理解为备选的leader，其按照最新offset是否同步作为入选标准
 * 1。 leader的写入offset是递增的，因此最理想的情况时维护最新的offset的ISR
 * 2。 但是在快速写入的情况下，offset是快速增加的，此时新的offset会覆盖旧的offset的ISR，并且ISR是需要同步到其余的broker节点的
 * 3。 如果同步的速率和mq写入的速率一样，那么节点内部会存在大量的请求用于同步ISR，也会耗费带宽，因此ISR的同步，必定是有一定时间间隔去触发的，这也是kafka的设计
 * 4。 kafka的isr还会有剔除的设计，功能更强大
 * 5。 本实现只实现定时同步，即一个异步线程负责replica，另一个定时任务5s同步一次当前的最新ISR到其余的broker
 *
 *
 * @Author: tangwc
 */
public class IsrSet {

    private long offset;
    private Set<String> isr = new HashSet<>();

    public long getOffset() {
        return offset;
    }

    public IsrSet setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public Set<String> getIsr() {
        return isr;
    }

    public IsrSet setIsr(Set<String> isr) {
        this.isr = isr;
        return this;
    }
}
