package com.wentry.wmq.domain.storage;

import com.wentry.wmq.common.Closable;
import com.wentry.wmq.transport.ReadRes;
import com.wentry.wmq.utils.file.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @Description:
 * @Author: tangwc
 */
public class DbReader implements Closable {

    private static final Logger log = LoggerFactory.getLogger(DbReader.class);

    private final String topic;
    private final int partition;

    private FileInputStreamHolder fileInputStreamHolder;

    public DbReader(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
        try {
            initFileStreamHolder();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initFileStreamHolder() throws FileNotFoundException {
        String path = FileUtils.fileDir(topic, partition);

        File directory = new File(path);
        if (!directory.exists() || !directory.isDirectory()) {
            FileUtils.createDirectoriesIfNotExist(path);
        }

        this.fileInputStreamHolder = new FileInputStreamHolder(0, topic, partition);
    }

    private long parseFileNameToLong(String fileName) {
        String numberPart = fileName.substring(0, fileName.indexOf(".wmqdb"));
        return Long.parseLong(numberPart);
    }

    public synchronized ReadRes read(long startOffset) {
        try {
            String ensure = ensureStartOffset(startOffset);
            if (!StringUtils.equals("ok", ensure)) {
                return new ReadRes().setFailMsg(ensure);
            }

            FileChannel channel = this.fileInputStreamHolder.getFileChannel();
            channel.position(startOffset - this.fileInputStreamHolder.getStartOffset());

            // 读取4个字节的数据长度
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            int bytesRead = channel.read(lengthBuffer);
            if (bytesRead < 4) {
                return new ReadRes().setData(null).setToEnd(true).setNextOffset(startOffset)
                        .setExtMsg("last file:" + fileInputStreamHolder.getFilePath() + ", size:" + fileInputStreamHolder.getFileChannel().size() + " bytes");
            }
            lengthBuffer.flip();
            int dataLength = lengthBuffer.getInt();

            // 验证数据长度是否合理
            if (dataLength <= 0) {
                log.error("Invalid data length: {}, path:{}", dataLength, this.fileInputStreamHolder.getFilePath());
                return new ReadRes().setFailMsg("无效的数据长度: " + dataLength);
            }

            // 读取实际的数据
            ByteBuffer dataBuffer = ByteBuffer.allocate(dataLength);
            bytesRead = channel.read(dataBuffer);
            if (bytesRead < dataLength) {
                log.error("length not eq, bytesRead:{},dataLength:{}, path:{}", bytesRead, dataLength, this.fileInputStreamHolder.getFilePath());
                throw new IOException("Failed to read enough data");
            }
            dataBuffer.flip();

            byte[] data = new byte[dataLength];
            dataBuffer.get(data);

            // 计算新的偏移量
            long newOffset = startOffset + 4 + dataLength;
            return new ReadRes().setData(data).setNextOffset(newOffset);
        } catch (IOException e) {
            log.error("Failed to read data from file: {}", this.fileInputStreamHolder.getFilePath(), e);
            return new ReadRes().setFailMsg("读取文件失败: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unknown exception occurred during read operation: {}", this.fileInputStreamHolder.getFilePath(), e);
            return new ReadRes().setFailMsg("未知异常: " + e.getMessage());
        }
    }

    private String ensureStartOffset(long startOffset) {
        //一个startOffset一定不会在eachFileSize之外，endOffset是有可能的
        while (startOffset > fileInputStreamHolder.getStartOffset() + DbConst.EACH_FILE_SIZE) {
            try {
                long fileSize = this.fileInputStreamHolder.getFileChannel().size();
                if (fileSize == 0) {
                    return "startOffset outbound, curr file startOffset is" + fileInputStreamHolder.getStartOffset() + ", fileSize:" + 0;
                }

                if (fileSize < DbConst.EACH_FILE_SIZE) {
                    return "startOffset outbound, curr bound is:" + fileInputStreamHolder.getStartOffset() + fileSize;
                }

                FileInputStreamHolder old = this.fileInputStreamHolder;
                //切换到下一个文件
                this.fileInputStreamHolder = new FileInputStreamHolder(
                        this.fileInputStreamHolder.getStartOffset() + fileSize,
                        topic,
                        partition
                );
                old.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "ok";
    }

    @Override
    public void close() {
        if (fileInputStreamHolder != null) {
            try {
                fileInputStreamHolder.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    public static void main(String[] args) throws IOException, InterruptedException {
//        DbReader reader = new DbReader("hello", 2);
//
//        // 假设我们已经知道要从哪个偏移量开始读取
//        long startOffset = 0;
//        int length = 4 + "Hello, World!".length(); // 预计的长度，包括4字节长度信息
//
//        ReadRes result = reader.read(startOffset);
//        System.out.println("Data read: " + new String(result.getData()));
//        System.out.println("Next offset: " + result.getNextOffset());
//
//        reader.close();
//    }
}
