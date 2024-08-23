package com.wentry.wmq.common;

/**
 * @Description:
 * @Author: tangwc
 *
 * 做一些资源关闭操作
 *
 */
public interface Closable {

    void close();

    /**
     * 越大的越后
     * @return
     */
    default int order(){
        return Integer.MIN_VALUE;
    }

}
