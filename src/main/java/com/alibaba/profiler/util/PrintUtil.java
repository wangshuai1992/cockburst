package com.alibaba.profiler.util;

import com.alibaba.profiler.config.QueueConfig;

/**
 * @author wxy on 16/6/4.
 */
public class PrintUtil {

    public final static String PROFILER = "analyzer-profiler ";

    public final static String INFO = PROFILER + " INFO ";

    public final static String WARN = PROFILER + " WARN ";

    public final static String DEBUG = PROFILER + " DEBUG ";

    public final static String ERROR = PROFILER + " ERROR ";

    public static void info(String message) {
        print(INFO + message);
    }

    public static void debug(String message) {
        print(DEBUG + message);
    }

    public static void warn(String message) {
        print(WARN + message);
    }

    public static void error(String message) {
        print(ERROR + message);
    }

    public static void print(String line) {
        if (QueueConfig.getInstance().isPrintExceptionStack()){
            System.out.println(line);
        }

    }
}
