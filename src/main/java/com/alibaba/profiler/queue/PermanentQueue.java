package com.alibaba.profiler.queue;

import java.io.InputStream;
import java.util.Properties;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.exception.FailedException;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy date 2017/04/26
 */
public class PermanentQueue {

    private static final String CONFIG_FILE = "profiler.properties";

    private volatile boolean configured = false;

    private PermanentQueue(){
        //load configuration
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE);
        Properties prop = new Properties();
        if (in == null) {
            LogUtil.warn("The profiler.properties not exist, all configure is default.");
            return;
        }

        if (isConfigured()) {
            return;
        }
        try {
            prop.load(in);
            setConfiguration(prop);
            LogUtil.info("Profiler configuration is successful. ");
        } catch (Exception e) {
            LogUtil.error("An error occurred when the configuration parameters. configure default" + e);
        } finally {
            try {
                in.close();
                prop.clear();
            } catch (Exception e) {
                // Nothing to do..
            }
        }
    }

    public void setConfiguration(Properties prop) {
        QueueConfig config = QueueConfig.getInstance();
        if (configured) {
            return;
        }

        config.setAll(prop);
        configured = true;
    }

    public boolean isConfigured() {
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
