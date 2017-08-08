package com.alibaba.profiler.queue;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.exception.FailedException;
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
                LogUtil.debug("queueFactory is destroying..........");
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

    public AbstractQueue getQueue(String key) throws FailedException {
        if (isStopped()) {
            throw new FailedException("queueFactory has been destroyed.please reload it");
        }
        try {
            FutureTask<AbstractQueue> old = queueHolders.get(key);
            if (old == null) {
                FutureTask<AbstractQueue> futureTask = new FutureTask<>(new QueueBuilder(key));
                old = queueHolders.putIfAbsent(key, futureTask);
                if (old == null) {
                    old = futureTask;
                    old.run();
                }
            }
            return futureGet(old);
        } catch (Exception e) {
            throw new FailedException("queueFactory get queue exception. " + e);
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
            AbstractQueue aq = new FileChannelQueue(queueName);
            aq.openQueue();
            return aq;
        }
    }

    private AbstractQueue futureGet(FutureTask<AbstractQueue> ft) {
        try {
            return ft.get();
        } catch (Exception e) {
            LogUtil.error("futureGet error. " + e);
        }
        throw new RuntimeException("Cannot create AsyncQueue. ");
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
