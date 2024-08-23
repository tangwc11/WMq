package com.wentry.wmq.transport;

import com.wentry.wmq.domain.registry.partition.PartitionInfo;

import java.io.Serializable;
import java.util.List;

/**
 * @Description:
 * @Author: tangwc
 */
public class PartitionSyncReq implements Serializable {
    private static final long serialVersionUID = -1966269831193753453L;

    List<PartitionInfo> partitions;
    long lastVersion;

    public long getLastVersion() {
        return lastVersion;
    }

    public PartitionSyncReq setLastVersion(long lastVersion) {
        this.lastVersion = lastVersion;
        return this;
    }

    public List<PartitionInfo> getPartitions() {
        return partitions;
    }

    public PartitionSyncReq setPartitions(List<PartitionInfo> partitions) {
        this.partitions = partitions;
        return this;
    }
}
