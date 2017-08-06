package com.alibaba.profiler.queue;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.exception.FailedException;
import com.alibaba.profiler.util.PrintUtil;

/**
 * @author wxy on 16/6/4.
 */
public class QueueFactory {

    private final ConcurrentHashMap<String, FutureTask<AbstractQueue>> queueHolders;
    private final AtomicBoolean stopped;
    private QueueConfig config = QueueConfig.getInstance();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                PrintUtil.debug("Profiler is shutting down..........");
                QueueFactory.getInstance().destroy();
            }
        }, "AsyncScribeSender-destroy"));
    }

    public static QueueFactory getInstance() {
        return AsyncScribeSenderHolder.INSTANCE;
    }

    private static class AsyncScribeSenderHolder {
        private static final QueueFactory INSTANCE = new QueueFactory();
    }

    private QueueFactory() {
        queueHolders = new ConcurrentHashMap<String, FutureTask<AbstractQueue>>();
        stopped = new AtomicBoolean(false);
    }

    public AbstractQueue getQueue(String key) throws FailedException {
        if (stopped.get()) {
            throw new FailedException("queueFactory has been destroyed.");
        }
        try {
            FutureTask<AbstractQueue> old = queueHolders.get(key);
            if (old == null) {
                FutureTask<AbstractQueue> futureTask = new FutureTask<AbstractQueue>(new AsyncQueueBuilder(key));
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

    static class AsyncQueueBuilder implements Callable<AbstractQueue> {
        private String queueName;

        public AsyncQueueBuilder(String queueName) {
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
            PrintUtil.error("futureGet error. " + e);
        }
        throw new RuntimeException("Cannot create AsyncQueue. ");
    }

    public void destroy() {
        stopped.set(true);
        Set<String> keySet = queueHolders.keySet();
        for (String key : keySet) {
            try {
                queueHolders.get(key).get().shutdown();
                PrintUtil.warn("Destroy the queue " + key);
            } catch (Exception e) {
                PrintUtil.error("Destroy the queue " + key + " failed. " + e);
                if (config.isPrintExceptionStack()) {
                    e.printStackTrace();
                }
            }
        }
    }
}
