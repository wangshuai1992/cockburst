package com.alibaba.profiler.manager;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.alibaba.profiler.queue.FileChannelQueue;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public class MemoryChannelWriter extends AbstractFileChannelWriter {
    private final ExecutorService writerSwapTask;
    private final ExecutorService writerFlushTask;
    private boolean stopped = false;
    private final ArrayBlockingQueue<String> wQueue;
    private final static long ASYNC_FLUSH_SECS = 2 * 1000;

    public MemoryChannelWriter(FileChannelQueue fileChannelQueue) {
        super(fileChannelQueue);
        final String queueName = fileChannelQueue.getQueueName();
        this.wQueue = new ArrayBlockingQueue<String>(100000);
        this.writerSwapTask = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Profiler-MemoryWriteSwapTask-" + queueName);
            }
        });
        this.writerFlushTask = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                return new Thread(r, "Profiler-MemoryWriteFlushTask-" + queueName);
            }
        });
    }

    @Override
    public void write(String message) {
        wQueue.add(message);
    }

    private void appendMessage(String message) {
        if (message == null || message.equals("")) {
            return;
        }
        byte[] cb = message.getBytes(Charset.forName("UTF-8"));
        if (cb.length == 0) {
            return;
        }
        int messageSize = 4 + cb.length;
        checkWriteChannel(messageSize);
        writeMappedByteBuffer.putInt(cb.length);
        writeMappedByteBuffer.put(cb);

        fileChannelQueue.getLatch().countDown();
    }

    private void swap() {
        while (!stopped) {
            try {
                String message = null;
                try {
                    message = wQueue.take();
                } catch (InterruptedException E) {
                    if (stopped) {
                        Iterator<String> it = wQueue.iterator();
                        while (it.hasNext()) {
                            appendMessage(it.next());
                        }
                        LogUtil.info("Service shutdown, append the remaining message. ");
                        return;
                    }
                }

                appendMessage(message);

            } catch (Exception e) {
                LogUtil.error("writer message exception. " + e);
            }
        }
    }

    private void flush() {
        while (!stopped) {
            try {
                if (writeMappedByteBuffer != null) {
                    writeMappedByteBuffer.force();
                }
            } catch (Exception e) {
                LogUtil.error("Writer flush exception. " + e);
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
        writerSwapTask.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    swap();
                } catch (Throwable t) {
                    LogUtil.error("swap() unhandle exception. " + t);
                }

                LogUtil.info("swap() stopped. ");
            }
        });

        writerFlushTask.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    flush();
                } catch (Throwable t) {
                    LogUtil.error("flush() unhandle exception. " + t);
                }

                LogUtil.info("flush() stopped. ");
            }
        });
    }

    @Override
    public void stop() {
        stopped = true;
        writerSwapTask.shutdownNow();
        writerFlushTask.shutdownNow();
        writeMappedByteBuffer.force();
        try {
            writeFileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Close write channel failed.", e);
        }
    }
}
