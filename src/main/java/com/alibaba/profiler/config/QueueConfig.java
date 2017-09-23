package com.alibaba.profiler.config;

import java.io.InputStream;
import java.util.Properties;

import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public class QueueConfig {

    /**default size 100M*/
    private static final String QUEUE_SEGMENT_SIZE = "104857600";
    private final static int MESSAGE_LENGTH_LIMIT = 5 * 1024 * 1024;
    private static final String CONFIG_FILE = "profiler.properties";

    private volatile boolean configured = false;

    private boolean printExceptionStack;
    private String metaPath;
    private String dataPath;
    private int queueSegmentSize;




    public boolean isPrintExceptionStack() {
        return printExceptionStack;
    }

    public void setPrintExceptionStack(boolean printExceptionStack) {
        this.printExceptionStack = printExceptionStack;
    }

    public String getMetaPath() {
        return metaPath;
    }

    public void setMetaPath(String metaPath) {
        this.metaPath = metaPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public int getQueueSegmentSize() {
        return queueSegmentSize;
    }

    public int getMessageLimit(){
        return MESSAGE_LENGTH_LIMIT;
    }

    public void setQueueSegmentSize(int queueSegmentSize) {
        this.queueSegmentSize = queueSegmentSize;
    }

    private QueueConfig() {
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
            LogUtil.info("QueueConfig configuration is successful. ");
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

    public static QueueConfig getInstance() {
        return QueueConfigHolder.INSTANCE;
    }

    private static class QueueConfigHolder {
        private static final QueueConfig INSTANCE = new QueueConfig();
    }


    public void setConfiguration(Properties prop) {
        if (configured) {
            return;
        }
        setAll(prop);
        configured = true;
    }

    public boolean isConfigured() {
        return configured;
    }
    public void setAll(Properties prop) {
        String appHome = getClass().getResource("/").getPath() + "../";
        String metaPath = prop.getProperty("metaPath", "./log/profiler/meta");
        if (!metaPath.startsWith("/")) {
            setMetaPath(appHome + metaPath);
        } else {
            setMetaPath(metaPath);
        }
        String dataPath = prop.getProperty("dataPath", "./log/profiler/data");
        if (!dataPath.startsWith("/")) {
            setDataPath(appHome + dataPath);
        } else {
            setDataPath(dataPath);
        }
        setQueueSegmentSize(Integer.parseInt(prop.getProperty("rotationSize", QUEUE_SEGMENT_SIZE
            + "")));
        setPrintExceptionStack(Boolean.parseBoolean(prop.getProperty("printExceptionStack", "true")));

    }

    public String showAll() {
        return toString();
    }

    @Override
    public String toString() {
        return "SenderConfig{" +
            "printExceptionStack=" + printExceptionStack +
            ", metaPath='" + metaPath + '\'' +
            ", dataPath='" + dataPath + '\'' +
            ", rotationSize=" + queueSegmentSize +
            '}';
    }
}
