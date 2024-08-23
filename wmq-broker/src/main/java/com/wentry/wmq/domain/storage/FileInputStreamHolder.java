package com.wentry.wmq.domain.storage;

import com.wentry.wmq.common.Closable;
import com.wentry.wmq.utils.file.FileUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * 文件读取持有者类。
 */
public class FileInputStreamHolder implements Closable {
    private long startOffset;
    private String topic;
    private int partition;
    private String filePath;
    private FileChannel fileChannel;

    public FileInputStreamHolder(long startOffset, String topic, int partition) throws FileNotFoundException {
        this.startOffset = startOffset;
        this.topic = topic;
        this.partition = partition;
        this.filePath = FileUtils.filePath(topic, partition, startOffset);
        FileUtils.createFileIfNotExist(filePath);
        this.fileChannel = new FileInputStream(filePath).getChannel();
    }

    public long getStartOffset() {
        return startOffset;
    }

    public FileInputStreamHolder setStartOffset(long startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public FileInputStreamHolder setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public FileInputStreamHolder setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getFilePath() {
        return filePath;
    }

    public FileInputStreamHolder setFilePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public FileInputStreamHolder setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        return this;
    }

    @Override
    public void close() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
