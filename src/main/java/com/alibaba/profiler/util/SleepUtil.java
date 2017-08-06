package com.alibaba.profiler.util;

import java.util.Random;

/**
 * @author wxy on 16/6/4.
 */
public class SleepUtil {
    public static void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (Exception ignore) {
            // Nothing to do..
        }
    }

    public static void randomSleep(int msec) {
        Random r = new Random();
        int sleepSec = r.nextInt(msec);
        sleep(sleepSec);
    }

    public static void delay(int base, int factor) {
        sleep(base * (factor + 1));
    }

    public static void main(String[] args) {
        randomSleep(1000);
    }
}
