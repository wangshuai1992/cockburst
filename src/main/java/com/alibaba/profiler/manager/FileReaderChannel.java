package com.alibaba.profiler.manager;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.Task;
import com.alibaba.profiler.queue.QueueChannel;
import com.alibaba.profiler.queue.MessageWrapper;
import com.alibaba.profiler.queue.Meta;
import com.alibaba.profiler.util.LogUtil;
import com.alibaba.profiler.util.SleepUtil;

/**
 * Description:read data to cache queue
 *
 * @author wxy
 *         create 2017-05-14 下午10:31
 */
public class FileReaderChannel implements Task {
    private final static int NO_DATA_WAIT_TIMEOUT = 2 * 1000;
    private final static int OPEN_CHANNEL_RETRY_COUNT = 3;
    private final static int OPEN_CHANNEL_RETRY_TIMEOUT = 5;
    private final static int MESSAGE_HEADER_LENGTH_LIMIT = 5 * 1024 * 1024;
    private String readFile;
    private int readPos;
    private boolean stopped = false;
    private boolean firstMessageInFile = false;
    private final ExecutorService readerTask;
    private final QueueChannel fileChannelQueue;
    private MappedByteBuffer readMappedByteBuffer;
    private FileChannel readFileChannel;

    public FileReaderChannel(QueueChannel fileChannelQueue) {
        this.fileChannelQueue = fileChannelQueue;
        findCurrentDataFile();
        //create a single thread
        this.readerTask = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<Runnable>(1), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "FileReaderChannel-ReaderTask-");
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
                LogUtil.error("Meta is " + readFile + ", but data files are not exists.");
            }
            readFile = null;
            readPos = 0;
            return;
        }
        if (readFile == null) {
            readFile = firstFile;
            readPos = 0;
        } else if (!readFile.equals(firstFile)) {
            LogUtil.warn("Meta " + readFile + " is not firstFile " + firstFile);
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
                LogUtil.error("Latch operation interruptted. " + e);
                return;
            }
            switchReadFile();
        }
        while (!stopped) {
            Long current = System.currentTimeMillis();
            openChannel();
            if (Thread.interrupted() || stopped) {
                return;
            }
            loadFromFile();
            if (Thread.interrupted() || stopped) {
                return;
            }
            switchReadFile();
            System.out.println("*************10M file consumer take time == " + (System.currentTimeMillis() - current));
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
                    LogUtil.error("SwitchReadFile error. " + e);
                }
            }
            readFile = dataFileManager.pollFirstFile();
            readPos = 0;
            LogUtil.info("Switched read file to " + readFile + ". ");
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
        throw new RuntimeException("Open read file channel failed.", t);
    }

    private int getHeader() {
        return (readMappedByteBuffer.remaining() < 4) ? 0 : readMappedByteBuffer.getInt();
    }

    private void loadFromFile() {
        DataFileManager dataFileManager = fileChannelQueue.getDataFileManager();
        firstMessageInFile = readPos == 0;
        readMappedByteBuffer.position(readPos);
        int length;
        while ((length = getHeader()) > 0 || dataFileManager.isEmpty()) {
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
                    LogUtil.error("Parse message header error, when tail the file. ");
                    SleepUtil.sleep(NO_DATA_WAIT_TIMEOUT);
                }
                return;
            }
            if (!messageBody(length)) {
                break;
            }
        }
        // Process time window: When new file create, the last file grow up.
        int leftLength;
        while ((leftLength = getHeader()) > 0) {
            if (Thread.interrupted() || stopped) {
                return;
            }
            if (leftLength > MESSAGE_HEADER_LENGTH_LIMIT) {
                LogUtil.error("Parse message header error, when process time window. ");
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
            LogUtil.error("Parse message header error, set position to the end. ");
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
            LogUtil.error("Message enqueue failed. " + e);
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
                    LogUtil.error("loadMessages() can not handle exception. " + t);
                }

                LogUtil.info("loadMessages() stopped. ");
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;
        readerTask.shutdownNow();
    }
}