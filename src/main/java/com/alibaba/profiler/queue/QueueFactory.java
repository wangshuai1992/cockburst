package com.alibaba.profiler.queue;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.exception.QueueException;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public class QueueFactory {

    private final ConcurrentHashMap<String, FutureTask<AbstractQueue>> queueHolders = new ConcurrentHashMap<>();
    private volatile boolean stopped = false;
    private QueueConfig config = QueueConfig.getInstance();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LogUtil.debug("QueueFactory is destroying..........");
                QueueFactory.getInstance().destroy();
            }
        }, "QueueFactory-destroy"));
    }

    public static QueueFactory getInstance() {
        return QueueFactoryHolder.INSTANCE;
    }

    private static class QueueFactoryHolder {
        private static final QueueFactory INSTANCE = new QueueFactory();
    }

    public AbstractQueue getQueue(String key) throws QueueException {
        if (isStopped()) {
            throw new QueueException("QueueFactory has been destroyed.please reload");
        }
        try {
            FutureTask<AbstractQueue> queueHolder = queueHolders.get(key);
            if (queueHolder == null) {
                queueHolders.putIfAbsent(key, new FutureTask<>(new QueueBuilder(key)));
                queueHolder = queueHolders.get(key);
                queueHolder.run();
            }
            return futureResult(queueHolder);
        } catch (Exception e) {
            throw new QueueException("QueueFactory get queue exception. " + e);
        }

    }

    private static class QueueBuilder implements Callable<AbstractQueue> {
        private String queueName;

        public QueueBuilder(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public AbstractQueue call() {
            //init permanent queue
            AbstractQueue aq = new QueueChannel(queueName);
            aq.openQueue();
            return aq;
        }
    }

    private AbstractQueue futureResult(FutureTask<AbstractQueue> ft) {
        try {
            return ft.get();
        } catch (Exception e) {
            LogUtil.error("futureResult error. " + e);
        }
        throw new RuntimeException("can not create queue. ");
    }

    public void destroy() {
        stopped = true;
        Set<String> keySet = queueHolders.keySet();
        for (String key : keySet) {
            try {
                queueHolders.get(key).get().shutdown();
                LogUtil.warn("Destroy the queue " + key);
            } catch (Exception e) {
                LogUtil.error("Destroy the queue " + key + " failed. " + e);
                if (config.isPrintExceptionStack()) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean isStopped(){
        return stopped;
    }
}
