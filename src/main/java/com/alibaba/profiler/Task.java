package com.alibaba.profiler;

/**
 * @author wxy on 16/6/4.
 */
public interface Task {
    /**
     * task start
     */
    void start();

    /**
     * task stop
     */
    void stop();
}
