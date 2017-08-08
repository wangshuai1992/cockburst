package com.alibaba.profiler.util;

import com.alibaba.profiler.config.QueueConfig;

/**
 * @author wxy on 16/6/4.
 */
public class LogUtil {


    public static void info(String message) {
        print(LogLevel.INFO.getValue() + message);
    }

    public static void debug(String message) {
        print(LogLevel.DEBUG.getValue() + message);
    }

    public static void warn(String message) {
        print(LogLevel.WARN.getValue() + message);
    }

    public static void error(String message) {
        print(LogLevel.ERROR.getValue() + message);
    }

    public static void print(String line) {
        if (QueueConfig.getInstance().isPrintExceptionStack()){
            System.out.println(line);
        }

    }

    enum LogLevel{
        INFO("COCKBURST INFO "),
        DEBUG("COCKBURST DEBUG "),
        WARN("COCKBURST WARN "),
        ERROR("COCKBURST ERROR ");

        private String value;

        LogLevel(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
