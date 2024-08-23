package com.wentry.wmq.domain.storage;


import com.wentry.wmq.common.Closable;
import com.wentry.wmq.transport.WriteRes;
import com.wentry.wmq.utils.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description:
 * @Author: tangwc
 */
public class DbWriter implements Closable {

    private static final Logger log = LoggerFactory.getLogger(DbWriter.class);

    private final String topic;
    private final int partition;

    //每个文件1024字节，用于测试文件切换

    private final AtomicLong offset = new AtomicLong();
    private volatile FileOutputStreamHolder fileOutputStreamHolder;
    private volatile FileOutputStreamHolder nextFileOutputStreamHolder;

    public DbWriter(String topic, int partition) throws FileNotFoundException {
        this.topic = topic;
        this.partition = partition;
        initFileStreamHolder();
    }

    public AtomicLong getOffset() {
        return offset;
    }

    /**
     * path目录下的文件名为一串数字，找出文件名最大的文件，并打开，同时赋值offset = 文件名的数值+文件中的bytes数
     */
    private void initFileStreamHolder() throws FileNotFoundException {
        String path = FileUtils.fileDir(topic, partition);

        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) {
            FileUtils.createDirectoriesIfNotExist(path);
        }

        // 获取目录下的所有文件
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".wmqdb"));
        if (files == null || files.length == 0) {
            //当前没有任何文件
            this.fileOutputStreamHolder = new FileOutputStreamHolder(0, topic, partition);
            offset.set(0);
            return;
        }

        // 找出文件名最大的文件
        File maxFile = Arrays.stream(files)
                .max(Comparator.comparingLong(x -> parseFileNameToLong(x.getName())))
                .orElseThrow(() -> new RuntimeException("Could not find the largest file"));

        // 计算文件名的数值加上文件的字节数
        long fileNameValue = parseFileNameToLong(maxFile.getName());
        long fileSize = maxFile.length();
        offset.set(fileNameValue + fileSize);
        log.info("DBWriter init , offset :{}, fileName:{}, fileSize:{}", offset.get(), maxFile.getAbsoluteFile() + "/" + maxFile.getName(), fileSize);
        this.fileOutputStreamHolder = new FileOutputStreamHolder(fileNameValue, topic, partition);
    }


    private long parseFileNameToLong(String fileName) {
        String numberPart = fileName.substring(0, fileName.indexOf(".wmqdb"));
        return Long.parseLong(numberPart);
    }

    public WriteRes write(byte[] bytes) {
        long newOffset;
        long preOffset = offset.get();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        ByteBuffer dataBuffer = ByteBuffer.wrap(bytes);

        try {
            int totalLength = 4 + bytes.length;
            FileChannel channel = getFos(totalLength).getChannel();
            // 移动文件指针到指定位置
            channel.position(offset.get());
            // 写入4个字节的数据长度
            lengthBuffer.putInt(bytes.length);
            lengthBuffer.flip(); // 切换到写模式
            newOffset = offset.get() + channel.write(lengthBuffer);

            // 将数据缓冲区中的数据写入文件
            newOffset += channel.write(dataBuffer);
            offset.set(newOffset);

            //切换到下一个文件
            if (nextFileOutputStreamHolder != null) {
                fileOutputStreamHolder.close();
                fileOutputStreamHolder = nextFileOutputStreamHolder;
                nextFileOutputStreamHolder = null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error writing to file", e);
        }

        return new WriteRes().setPreOffset(preOffset).setLatestOffset(newOffset);
    }

    private FileOutputStream getFos(int length) throws FileNotFoundException {

        if ((offset.get() + length) >= (fileOutputStreamHolder.getStartOffset() + DbConst.EACH_FILE_SIZE)) {
            //预创建下一个
            nextFileOutputStreamHolder = new FileOutputStreamHolder(offset.get() + length, topic, partition);
        }
        return fileOutputStreamHolder.getFos();
    }
    @Override
    public void close() {
        if (fileOutputStreamHolder != null) {
            fileOutputStreamHolder.close();
        }
    }
}
