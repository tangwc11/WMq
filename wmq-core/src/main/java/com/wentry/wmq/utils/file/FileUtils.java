package com.wentry.wmq.utils.file;

import java.io.File;
import java.util.Arrays;

/**
 * @Description:
 * @Author: tangwc
 */
public class FileUtils {

    public static String DB_BASE_DIR = "wmq/db";
    public static final String DB_FILE_SUFFIX = ".wmqdb";
    public static final int dbFileNameLength = 9;

    public static String filePath(String topic, int partition, long startOffset) {
        return String.join("/", DB_BASE_DIR, topic, String.valueOf(partition), fillZero(startOffset) + DB_FILE_SUFFIX);
    }

    private static String fillZero(long startOffset) {
        /**
         * todo wch 这里简单设置一下个达不到的长度
         * 实际设计需要考虑上限，即到达上限之后，回头覆盖第一个文件，这又涉及当前offset对应的文件的读取，所以先不处理这些逻辑
         * 一个partition，最多写1GB的文件，即1000000000bytes
         * 所以最多9位就够了（ 9 = 1GB换算成bytes 数字的长度-1 ）
         */

        String offsetStr = String.valueOf(startOffset);
        if (offsetStr.length() > dbFileNameLength) {
            return offsetStr;
        }
        //不足9位，左边用0补齐
        char[] chars = new char[dbFileNameLength - offsetStr.length()];
        Arrays.fill(chars, '0');
        return String.valueOf(chars) + offsetStr;
    }

    public static String fileDir(String topic, int partition) {
        return String.join("/", DB_BASE_DIR, topic, String.valueOf(partition));
    }

    public static void createDirectoriesIfNotExist(String filePath) {
        File file = new File(filePath);
        File parentDirectory = file.getParentFile();
        if (parentDirectory != null && !parentDirectory.exists()) {
            boolean created = parentDirectory.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create directories: " + parentDirectory.getAbsolutePath());
            }
        }
    }

    public static void createFileIfNotExist(String filePath) {
        File file = new File(filePath);
        File parentDirectory = file.getParentFile();
        if (parentDirectory != null && !parentDirectory.exists()) {
            boolean created = parentDirectory.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create directories: " + parentDirectory.getAbsolutePath());
            }
        }

        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
