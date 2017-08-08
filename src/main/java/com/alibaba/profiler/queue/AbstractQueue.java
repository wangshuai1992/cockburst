package com.alibaba.profiler.queue;

import com.alibaba.profiler.exception.FailedException;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public abstract class AbstractQueue implements AsyncTask {
    protected final String queueName;
    protected boolean stopped = false;

    public AbstractQueue(final String queueName) {
        this.queueName = queueName;
    }

    /**
     * add data
     *
     * @param data
     */
    public abstract void offer(String data);

    /**
     * get data
     *
     * @return data
     */
    public abstract MessageWrapper get();

    /**
     * update meta info
     * delete all queue files which all has consumed
     */
    public abstract void confirmLastMessage();

    public String pop() throws FailedException {
        MessageWrapper messageWrapper = get();
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message get from the queue.");
        }

        String message;
        try {
            message = new String(buff, "UTF-8");
            confirmLastMessage();
            return message;
        } catch (Exception e) {
            //convert message exception ,abandon it
            confirmLastMessage();
            throw new FailedException("Message type conversion error occurred. ", e);
        }
    }

    public void pop(Handler handler) throws FailedException {
        MessageWrapper messageWrapper = get();
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message get from the queue.");
        }

        String message;
        try {
            message = new String(buff, "UTF-8");
        } catch (Exception e) {
            //��Ϣת��ʧ�ܣ�����
            confirmLastMessage();
            throw new FailedException("Message type conversion error occurred. ", e);
        }
        handler.handerData(message);
        confirmLastMessage();
    }

    public void shutdown() {
        stopped = true;
        stop();
    }

    public void openQueue() {
        start();
    }

}
