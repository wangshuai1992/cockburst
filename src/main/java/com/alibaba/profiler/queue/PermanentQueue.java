package com.alibaba.profiler.queue;

import com.alibaba.profiler.Handler;
import com.alibaba.profiler.exception.QueueException;

/**
 * @author wxy date 2017/04/26
 */
public class PermanentQueue {

    private static QueueFactory queueFactory = QueueFactory.getInstance();

    /**
     *
     * @param queueCategory 队列名称
     * @param data 数据
     * @throws QueueException
     */
    public static void offer(String queueCategory, String data) throws QueueException {
        queueFactory.getQueue(queueCategory).offer(data);
    }

    /**
     *
     * @param queueCategory 队列名称
     * @throws QueueException
     */
    public static String pop(String queueCategory) throws QueueException {
        return queueFactory.getQueue(queueCategory).pop();
    }

    public static String pop(String queueCategory,Handler handler) throws QueueException {
        return queueFactory.getQueue(queueCategory).pop(handler);
    }

    /**
     *
     * @param queueCategory 队列名称
     * @throws QueueException
     */
    public static String take(String queueCategory) throws QueueException {
        return queueFactory.getQueue(queueCategory).take();
    }

    public static String take(String queueCategory,Handler handler) throws QueueException {
        return queueFactory.getQueue(queueCategory).take(handler);
    }

}
