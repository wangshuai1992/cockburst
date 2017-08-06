package com.alibaba.profiler.queue;

import java.io.InputStream;
import java.util.Properties;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.exception.FailedException;
import com.alibaba.profiler.util.PrintUtil;

/**
 * @author wxy date 2017/04/26
 */
public class PermanentQueue {

    private static final String PROFILER_CONFIG_FILE = "profiler.properties";

    private volatile boolean configured = false;

    private PermanentQueue(){
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROFILER_CONFIG_FILE);
        Properties prop = new Properties();
        if (isConfigured()) {
            return;
        }
        if (in == null) {
            //todo something
            PrintUtil.warn("The profiler.properties is not exist, maybe configure by code.");
            return;
        }

        try {
            prop.load(in);
            QueueConfig.getInstance().setAll(prop);
            configured = true;
            PrintUtil.info("Profiler configuration is successful. ");
        } catch (Exception e) {
            //todo something
            PrintUtil.error("An error occurred when the configuration parameters. " + e);
        } finally {
            try {
                in.close();
                prop.clear();
            } catch (Exception e) {
                // Nothing to do..
            }
        }
    }

    public synchronized void setConfiguration(Properties prop) {
        QueueConfig config = QueueConfig.getInstance();
        if (configured) {
            return;
        }

        config.setAll(prop);
        configured = true;
    }

    public synchronized boolean isConfigured() {
        return configured;
    }



    public static PermanentQueue getInstance() {
        return PermanentQueueHolder.INSTANCE;
    }

    private static class PermanentQueueHolder {
        private static final PermanentQueue INSTANCE = new PermanentQueue();
    }

    public void offer(String queueCategory, String data) throws FailedException {
        QueueFactory.getInstance().getQueue(queueCategory).offer(data);
    }

    public String pop(String queueCategory) throws FailedException {
        return QueueFactory.getInstance().getQueue(queueCategory).pop();
    }

    public void pop(String queueCategory,Handler handler){

    }


}
