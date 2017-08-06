package com.alibaba.profiler.manager;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.alibaba.profiler.manager.AbstractFileChannelWriter;
import com.alibaba.profiler.queue.FileChannelQueue;
import com.alibaba.profiler.util.PrintUtil;

/**
 * @author wxy on 16/6/4.
 */
public class MMapChannelWriter extends AbstractFileChannelWriter {
    private final ExecutorService writerTask;
    private boolean stopped = false;
    private final static long ASYNC_FLUSH_SECS = 2 * 1000;

    public MMapChannelWriter(FileChannelQueue fileChannelQueue) {
        super(fileChannelQueue);
        final String queueName = fileChannelQueue.getQueueName();
        this.writerTask = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                return new Thread(r, "Profiler-MMapWriteTask-" + queueName);
            }
        });
    }

    @Override
    public synchronized void write(String message) {
        try {
            byte[] cb = message.getBytes(Charset.forName("UTF-8"));
            if (cb.length == 0) {
                return;
            }
            int messageSize = 4 + cb.length;
            checkWriteChannel(messageSize);

            writeMappedByteBuffer.putInt(cb.length);
            writeMappedByteBuffer.put(cb);
        } catch (Exception e) {
            throw new RuntimeException("Write message failed.", e);
        }

        fileChannelQueue.getLatch().countDown();
    }

    private void asyncFlush() {
        while (!stopped) {
            try {
                if (writeMappedByteBuffer != null) {
                    writeMappedByteBuffer.force();
                }
            } catch (Exception e) {
                PrintUtil.error("Writer flush exception. " + e);
            } finally {
                try {
                    Thread.sleep(ASYNC_FLUSH_SECS);
                } catch (InterruptedException e) {
                    // Nothing to do..
                }
            }
        }
    }

    @Override
    public void start() {
        writerTask.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    asyncFlush();
                } catch (Throwable t) {
                    PrintUtil.error("asyncFlush() unhandle exception. " + t);
                }

                PrintUtil.info("asyncFlush() stopped. ");
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;
        writerTask.shutdownNow();
        writeMappedByteBuffer.force();
        try {
            writeFileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Close write channel failed.", e);
        }
    }
}
