package com.wentry.wmq.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);

    /**
     * 将 Date 对象转换为字符串
     *
     * @param date 要转换的 Date 对象
     * @return 格式化后的字符串
     */
    public static String format(Date date) {
        return sdf.format(date);
    }

    /**
     * 将 Date 对象转换为字符串，使用自定义格式
     *
     * @param date   要转换的 Date 对象
     * @param format 日期格式字符串
     * @return 格式化后的字符串
     */
    public static String format(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 将字符串转换为 Date 对象
     *
     * @param dateString 要转换的日期字符串
     * @return 转换后的 Date 对象
     * @throws ParseException 如果无法解析日期字符串
     */
    public static Date parse(String dateString) throws ParseException {
        return sdf.parse(dateString);
    }

    /**
     * 将字符串转换为 Date 对象，使用自定义格式
     *
     * @param dateString 要转换的日期字符串
     * @param format     日期格式字符串
     * @return 转换后的 Date 对象
     * @throws ParseException 如果无法解析日期字符串
     */
    public static Date parse(String dateString, String format) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.parse(dateString);
    }

    /**
     * 获取当前时间的字符串表示
     *
     * @return 当前时间的字符串表示
     */
    public static String getCurrentTime() {
        return format(new Date());
    }

    /**
     * 获取当前时间的字符串表示，使用自定义格式
     *
     * @param format 日期格式字符串
     * @return 当前时间的字符串表示
     */
    public static String getCurrentTime(String format) {
        return format(new Date(), format);
    }
}
