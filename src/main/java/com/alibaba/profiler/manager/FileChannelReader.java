package com.alibaba.profiler.manager;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.queue.AsyncTask;
import com.alibaba.profiler.queue.FileChannelQueue;
import com.alibaba.profiler.queue.MessageWrapper;
import com.alibaba.profiler.queue.Meta;
import com.alibaba.profiler.util.PrintUtil;
import com.alibaba.profiler.util.SleepUtil;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-23
 * Time: ÏÂÎç4:56
 * To change this template use File | Settings | File Templates.
 */
public class FileChannelReader implements AsyncTask {
    private final static int NO_DATA_WAIT_TIMEOUT = 2 * 1000;
    private final static int OPEN_CHANNEL_RETRY_COUNT = 3;
    private final static int OPEN_CHANNEL_RETRY_TIMEOUT = 5;
    private final static int MESSAGE_HEADER_LENGTH_LIMIT = 5 * 1024 * 1024;
    private String readFile;
    private int readPos;
    private boolean stopped = false;
    private boolean firstMessageInFile = false;
    private final ExecutorService readerTask;
    private final FileChannelQueue fileChannelQueue;
    private MappedByteBuffer readMappedByteBuffer;
    private FileChannel readFileChannel;

    public FileChannelReader(FileChannelQueue fileChannelQueue) {
        this.fileChannelQueue = fileChannelQueue;
        final String queueName = fileChannelQueue.getQueueName();
        findCurrentDataFile();
        this.readerTask = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Profiler-ReadTask-" + queueName);
            }
        });
    }

    private void findCurrentDataFile() {
        Meta meta = fileChannelQueue.getMetaManager().get();
        DataFileManager dataFileManager = fileChannelQueue.getDataFileManager();
        readPos = meta.getReadPos();
        readFile = meta.getFileName();
        String firstFile = dataFileManager.pollFirstFile();
        if (firstFile == null) {
            if (readFile != null) {
                PrintUtil.error("Meta is " + readFile + ", but data files are not exists.");
            }
            readFile = null;
            readPos = 0;
            return;
        }
        if (readFile == null) {
            readFile = firstFile;
            readPos = 0;
        } else if (!readFile.equals(firstFile)) {
            PrintUtil.warn("Meta " + readFile + " is not firstFile " + firstFile);
            File f = new File(readFile);
            // If exists, find it.
            if (f.exists() && dataFileManager.findFile(readFile)) {
                return;
            }
            readFile = firstFile;
            readPos = 0;
        }
    }

    private void loadMessages() {
        if (readFile == null) {
            try {
                fileChannelQueue.getLatch().await();
            } catch (InterruptedException e) {
                PrintUtil.error("Latch operation interruptted. " + e);
                return;
            }
            switchReadFile();
        }
        while (!stopped) {
            openChannel();
            if (Thread.interrupted() || stopped) {
                return;
            }
            loadFromFile();
            if (Thread.interrupted() || stopped) {
                return;
            }
            switchReadFile();
        }
    }

    private void switchReadFile() {
        DataFileManager dataFileManager = fileChannelQueue.getDataFileManager();
        if (!dataFileManager.isEmpty()) {
            if (readFileChannel != null) {
                try {
                    readMappedByteBuffer.force();
                    readFileChannel.close();
                } catch (Exception e) {
                    PrintUtil.error("SwitchReadFile error. " + e);
                }
            }
            readFile = dataFileManager.pollFirstFile();
            readPos = 0;
            PrintUtil.info("Switched read file to " + readFile + ". ");
        }
    }

    private void openChannel() {
        Throwable t = null;
        int rotationSize = QueueConfig.getInstance().getRotationSize();

        for (int i = 0; i < OPEN_CHANNEL_RETRY_COUNT; i++) {
            try {
                readFileChannel = new RandomAccessFile(readFile, "rw").getChannel();
                readMappedByteBuffer = readFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, rotationSize);
                return;
            } catch (Exception e) {
                t = e;
                SleepUtil.delay(OPEN_CHANNEL_RETRY_TIMEOUT, i);
            }
        }
        if (t != null) {
            throw new RuntimeException("Open read file channel failed.", t);
        }
    }

    private int getHeader() {
        return (readMappedByteBuffer.remaining() < 4) ? 0 : readMappedByteBuffer.getInt();
    }

    private void loadFromFile() {
        DataFileManager dataFileManager = fileChannelQueue.getDataFileManager();
        firstMessageInFile = readPos == 0;
        readMappedByteBuffer.position(readPos);
        int length;
        while ((length = getHeader()) > 0
            || dataFileManager.isEmpty()) {
            if (Thread.interrupted() || stopped) {
                return;
            }
            if (length <= 0) {
                readMappedByteBuffer.position(readPos);
                SleepUtil.sleep(NO_DATA_WAIT_TIMEOUT);
                continue;
            }
            if (length > MESSAGE_HEADER_LENGTH_LIMIT) {
                while (dataFileManager.isEmpty()) {
                    PrintUtil.error("Parse message header error, when tail the file. ");
                    SleepUtil.sleep(NO_DATA_WAIT_TIMEOUT);
                }
                return;
            }
            if (!messageBody(length)) {
                break;
            }
        }
        // Process time window: When new file create, the last file grow up.
        int leftLength = 0;
        while ((leftLength = getHeader()) > 0) {
            if (Thread.interrupted() || stopped) {
                return;
            }
            if (leftLength > MESSAGE_HEADER_LENGTH_LIMIT) {
                PrintUtil.error("Parse message header error, when process time window. ");
                return;
            }
            if (!messageBody(leftLength)) {
                break;
            }
        }
    }

    private boolean messageBody(int length) {
        if (length > MESSAGE_HEADER_LENGTH_LIMIT) {
            readMappedByteBuffer.position(QueueConfig.getInstance().getRotationSize());
            PrintUtil.error("Parse message header error, set position to the end. ");
            return false;
        }
        byte[] content = new byte[length];
        readMappedByteBuffer.get(content);
        readPos = readMappedByteBuffer.position();
        messageEnqueue(content);
        return true;
    }

    private void messageEnqueue(byte[] content) {
        MessageWrapper messageWrapper = new MessageWrapper(content, firstMessageInFile
            , readPos, readFile);
        firstMessageInFile = false;
        try {
            fileChannelQueue.getQueue().put(messageWrapper);
        } catch (InterruptedException e) {
            PrintUtil.error("Message enqueue failed. " + e);
        }
    }

    @Override
    public void start() {
        readerTask.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    loadMessages();
                } catch (Throwable t) {
                    PrintUtil.error("loadMessages() unhandle exception. " + t);
                }

                PrintUtil.info("loadMessages() stopped. ");
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;
        readerTask.shutdownNow();
    }
}