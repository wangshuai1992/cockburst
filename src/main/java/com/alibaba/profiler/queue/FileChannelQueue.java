package com.alibaba.profiler.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.manager.AbstractFileChannelWriter;
import com.alibaba.profiler.manager.DataFileManager;
import com.alibaba.profiler.manager.FileChannelReader;
import com.alibaba.profiler.manager.MetaManager;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public class FileChannelQueue extends AbstractQueue {

    private final MetaManager metaManager;
    private final DataFileManager dataFileManager;
    private final AbstractFileChannelWriter writer;
    private final FileChannelReader reader;
    private MessageWrapper lastMessage;
    private final LinkedBlockingQueue<MessageWrapper> queue;
    private final String queueName;
    private final CountDownLatch latch = new CountDownLatch(1);
    private QueueConfig config = QueueConfig.getInstance();

    public FileChannelQueue(String queueName) {
        super(queueName);
        this.queueName = queueName;
        this.metaManager = new MetaManager(queueName);
        this.dataFileManager = new DataFileManager(queueName);
        this.writer = AbstractFileChannelWriter.createWriter(config.getPattern(), this);
        this.reader = new FileChannelReader(this);
        this.queue = new LinkedBlockingQueue<>(100000);
    }

    public String getQueueName() {
        return queueName;
    }

    public MetaManager getMetaManager() {
        return metaManager;
    }

    public DataFileManager getDataFileManager() {
        return dataFileManager;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public LinkedBlockingQueue<MessageWrapper> getQueue() {
        return queue;
    }

    @Override
    public void offer(String data) {
        writer.write(data);
    }

    @Override
    public MessageWrapper get() {
        MessageWrapper messageWrapper = null;
        try {
            //todo luxi 这有线程阻塞的风险
            messageWrapper = queue.take();
            lastMessage = messageWrapper;
        } catch (Exception e) {
            LogUtil.warn("Interrupted when take message from FileChannelQueue. " + e);
        }
        return messageWrapper;
    }

    @Override
    public void start() {
        reader.start();
        writer.start();
    }

    @Override
    public void stop() {
        reader.stop();
        writer.stop();
        metaManager.close();
    }
    @Override
    public void confirmLastMessage() {
        if (lastMessage != null) {
            metaManager.update(lastMessage.getEndPos(), lastMessage.getCurrentFile());
            if (lastMessage.isFirstMessage()) {
                dataFileManager.deleteOlderFiles(lastMessage.getCurrentFile());
            }
        }
    }
}
