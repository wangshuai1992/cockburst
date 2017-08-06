package com.alibaba.profiler.queue;

/**
 * @author wxy on 16/6/4.
 */
public interface AsyncTask {
    /**
     * task start
     */
    void start();

    /**
     * task stop
     */
    void stop();
}
