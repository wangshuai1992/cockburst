package com.alibaba.profiler.manager;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.queue.AsyncTask;
import com.alibaba.profiler.queue.FileChannelQueue;

/**
 * @author wxy on 16/6/4.
 */
public abstract class AbstractFileChannelWriter implements AsyncTask {
    protected MappedByteBuffer writeMappedByteBuffer;
    protected FileChannel writeFileChannel;
    protected final FileChannelQueue fileChannelQueue;

    public AbstractFileChannelWriter(FileChannelQueue fileChannelQueue) {
        this.fileChannelQueue = fileChannelQueue;
    }

    public static AbstractFileChannelWriter createWriter(String name, FileChannelQueue fileChannelQueue) {
        /*if (SenderConfig.SENDER_PATTERN_MEMORY.equals(name)) {
            return new MemoryChannelWriter(fileChannelQueue);
        }*/
        return new MMapChannelWriter(fileChannelQueue);
    }

    protected void checkWriteChannel(int messageSize) {
        int rotationSize = QueueConfig.getInstance().getRotationSize();
        try {
            if (writeFileChannel == null) {
                writeFileChannel = new RandomAccessFile(fileChannelQueue.getDataFileManager()
                    .createRotationFile(), "rw").getChannel();
                writeMappedByteBuffer = writeFileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, rotationSize);
            }
            if ((writeMappedByteBuffer.position() + messageSize) > rotationSize) {
                writeMappedByteBuffer.force();
                writeFileChannel.close();
                writeFileChannel = new RandomAccessFile(fileChannelQueue.getDataFileManager()
                    .createRotationFile(), "rw").getChannel();
                writeMappedByteBuffer = writeFileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, rotationSize);
            }
        } catch (Exception e) {
            throw new RuntimeException("Create write channel failed", e);
        }
    }

    /**
     * write message to queue
     * @param message data
     */
    public abstract void write(String message);
}
