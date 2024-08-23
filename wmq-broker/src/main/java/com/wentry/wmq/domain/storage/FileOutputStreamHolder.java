package com.wentry.wmq.domain.storage;

import com.wentry.wmq.common.Closable;
import com.wentry.wmq.utils.file.FileUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * @Description:
 * @Author: tangwc
 */
public class FileOutputStreamHolder implements Closable {

    private long startOffset;
    private String topic;
    private int partition;
    private String filePath;
    FileOutputStream fos;

    public FileOutputStreamHolder(long startOffset, String topic, int partition) throws FileNotFoundException {
        this.startOffset = startOffset;
        this.topic = topic;
        this.partition = partition;
        this.filePath = FileUtils.filePath(topic, partition, startOffset);
        FileUtils.createFileIfNotExist(filePath);
        this.fos = new FileOutputStream(filePath, true);
    }

    public long getStartOffset() {
        return startOffset;
    }

    public FileOutputStreamHolder setStartOffset(long startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public FileOutputStreamHolder setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public int getPartition() {
        return partition;
    }

    public FileOutputStreamHolder setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public String getFilePath() {
        return filePath;
    }

    public FileOutputStreamHolder setFilePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public FileOutputStream getFos() {
        return fos;
    }

    public FileOutputStreamHolder setFos(FileOutputStream fos) {
        this.fos = fos;
        return this;
    }

    @Override
    public void close() {
        if (fos != null) {
            try {
                fos.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
