package com.alibaba.profiler.queue;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.util.PrintUtil;
import com.alibaba.profiler.util.SleepUtil;

/**
 * @author wxy on 16/6/4.
 */
public class FileMonitor implements AsyncTask {
    private final String filePath;
    private final ExecutorService monitorTask;
    private boolean stopped = false;
    private final static double WARN_SIZE = 5120;
    /**10min*/
    private final static long MONITOR_TIME = 10 * 60 * 1000;

    public FileMonitor() {
        this.filePath = QueueConfig.getInstance().getDataPath();
        this.monitorTask = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Profiler-monitorTask-" + filePath);
            }
        });

    }

    private void monitor() {
        File file = new File(filePath);
        try{
            while (!stopped) {
                double fileSize = getDirSize(file);
                if (fileSize > WARN_SIZE ) {
                    sendMessage("Permanent Queue file size is "+fileSize+"M, over Threshold "+WARN_SIZE);
                }
                SleepUtil.sleep(MONITOR_TIME);
            }
        }catch (Exception e){
            PrintUtil.error("monitor thread run error"+e.getCause());
        }

    }

    private double getDirSize(File file) {
        //判断文件是否存在
        double size = 0;
        if (!file.exists()) {
            PrintUtil.error("monitor dir is not exist");
            return size;
        }
        //如果是目录则递归计算其内容的总大小
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (null == children||children.length==0){
                return size;
            }
            for (File f : children){
                size += getDirSize(f);
            }
            return size;
        } else {//如果是文件则直接返回其大小,以“兆”为单位
            size = (double) file.length() / 1024 / 1024;
            return size;
        }

    }

    @Override
    public void start() {
        monitorTask.submit(new Runnable() {
            @Override
            public void run() {
                monitor();
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;
        monitorTask.shutdown();
    }

    private  void sendMessage(String message) {
        PrintUtil.error(message);
    }
}
