package com.wentry.wmq.openapi;


/**
 * @Description:
 *  消息监听器，用于编写实际业务
 *  1. 消费者组用于提升消费速率，同一个group里的consumer瓜分partition
 *  2. 每个consumer可以配置消费模式，顺序还是并发，用于利用多线程提升性能
 *  3. 默认onMessage需要主动ack，才会下发下一个消息
 *  . kafka和rocketmq有推模式和拉模式，这里只实现拉模式，即根据consumer的消费速率主动拉取消费
 * @Author: tangwc
 *
 */
public interface WmqListener {

    /**
     * 监听的是哪个topic
     */
    String topic();

    /**
     * 消费者组
     */
    String consumerGroup();


    /**
     * 消费逻辑
     */
    void onMessage(String msg);


}
