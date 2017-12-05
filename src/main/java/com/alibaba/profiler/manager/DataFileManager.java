package com.alibaba.profiler.manager;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.util.LogUtil;

/**
 * DataFileManager
 * @author wxy
 */
public class DataFileManager {
    private final String dataPath;
    private final String dataFileName;
    private final static String DATA_FILE_SUFFIX = ".data";
    /**
     * 队列所有的文件块
     */
    private final TreeSet<String> files;
    /**
     * 已经消费过的队列文件块
     */
    private final TreeSet<String> oldFiles;

    public DataFileManager(String queueName) {
        this.dataPath = QueueConfig.getInstance().getDataPath() + "/";
        this.dataFileName = queueName + DATA_FILE_SUFFIX;
        this.files = new TreeSet<>();
        this.oldFiles = new TreeSet<>();
        loadFiles();
    }

    /**
     * load data files
     */
    private void loadFiles() {
        File dir = new File(dataPath);
        if (!dir.exists()) {
            return;
        }
        String[] nameList = dir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.contains(dataFileName));
            }
        });
        for (String fileName : nameList) {
            String fullPath = dataPath + fileName;
            files.add(fullPath);
        }
    }

    /**
     * create new data file
     * @return data file's name
     */
    public synchronized String createRotationFile() {
        long count = 0;
        if (!files.isEmpty()) {
            String lastFileName = files.last();
            count = Long.valueOf(lastFileName.substring(lastFileName.lastIndexOf(".") + 1)) + 1;
        } else if (!oldFiles.isEmpty()) {
            String lastFileName = oldFiles.last();
            count = Long.valueOf(lastFileName.substring(lastFileName.lastIndexOf(".") + 1)) + 1;
        }

        File dir = new File(dataPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new RuntimeException("Cannot create data directory: " + dataPath);
        }

        String newFileName = dataPath + dataFileName + "." + String.format("%019d", count);
        files.add(newFileName);
        return newFileName;
    }

    public synchronized boolean findFile(String findName) {
        boolean isFound = false;
        if (findName == null) {
            return false;
        }
        List<String> tempFileList = new ArrayList<>();
        String fileName;
        while ((fileName = files.pollFirst()) != null) {
            tempFileList.add(fileName);
            if (fileName.equals(findName)) {
                isFound = true;
                break;
            }
        }

        if (!isFound) {
            for (String tempFile : tempFileList) {
                files.add(tempFile);
            }
        } else {
            for (String tempFile : tempFileList) {
                oldFiles.add(tempFile);
            }
        }

        return isFound;
    }

    public synchronized String pollFirstFile() {
        String fileName = files.pollFirst();
        if (fileName != null) {
            oldFiles.add(fileName);
        }
        return fileName;
    }

    public synchronized void deleteOlderFiles(String filePath) {
        if (oldFiles.size() == 0) {
            return;
        }
        String first = oldFiles.first();
        if (first.equals(filePath)) {
            return;
        }
        NavigableSet<String> subSet = oldFiles.subSet(first, true, filePath, false);
        List<String> deleteList = new ArrayList<>();
        for (String deletePath : subSet) {
            if (!new File(deletePath).delete()) {
                LogUtil.error("Delete the old file " + deletePath + " failed.");
            } else {
                deleteList.add(deletePath);
            }
        }

        // Delete from tree
        for (String path : deleteList) {
            oldFiles.remove(path);
        }
    }

    public synchronized boolean isEmpty() {
        return files.isEmpty();
    }
}
