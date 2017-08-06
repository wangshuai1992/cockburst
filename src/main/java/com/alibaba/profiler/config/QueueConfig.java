package com.alibaba.profiler.config;

import java.util.Properties;


/**
 * @author wxy on 16/6/4.
 */
public class QueueConfig {

    private static final String QUEUE_SEGMENT_SIZE = "10485760";

    private boolean printExceptionStack;
    private String metaPath;
    private String dataPath;
    private int rotationSize;
    private int readBuffer;

    private String pattern;
    private int batch;




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

    public int getRotationSize() {
        return rotationSize;
    }

    public void setRotationSize(int rotationSize) {
        this.rotationSize = rotationSize;
    }

    public int getReadBuffer() {
        return readBuffer;
    }

    public void setReadBuffer(int readBuffer) {
        this.readBuffer = readBuffer;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }



    public int getBatch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    private QueueConfig() {

    }

    public static final QueueConfig getInstance() {
        return SenderConfigHolder.INSTANCE;
    }

    private static class SenderConfigHolder {
        private static final QueueConfig INSTANCE = new QueueConfig();
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
        //setPattern(prop.getProperty("pattern"));
        setRotationSize(Integer.parseInt(prop.getProperty("rotationSize", QUEUE_SEGMENT_SIZE
            + "")));
        setReadBuffer(Integer.parseInt(prop.getProperty("readBuffer", "2048")));
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
            ", rotationSize=" + rotationSize +
            ", readBuffer=" + readBuffer +
            ", pattern='" + pattern + '\'' +
            ", batch=" + batch +
            '}';
    }
}
