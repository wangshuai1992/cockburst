package com.alibaba.profiler.queue;

import com.alibaba.profiler.exception.QueueException;

/**
 * @author wxy.
 */
public class PermanentQueue {

    private static QueueFactory queueFactory = QueueFactory.getInstance();

    /**
     * write data to queue
     * @param queueCategory 队列名称
     * @param data 数据
     * @throws QueueException
     */
    public static void offer(String queueCategory, String data) throws QueueException {
        queueFactory.getQueue(queueCategory).offer(data);
    }

    /**
     * pop data from queue
     * @param queueCategory 队列名称
     * @throws QueueException
     */
    public static String pop(String queueCategory) throws QueueException {
        return queueFactory.getQueue(queueCategory).pop();
    }

    /**
     * take data from queue
     * @param queueCategory 队列名称
     * @throws QueueException
     */
    public static String take(String queueCategory) throws QueueException {
        return queueFactory.getQueue(queueCategory).take();
    }

}
