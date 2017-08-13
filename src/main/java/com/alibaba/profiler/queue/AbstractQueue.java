package com.alibaba.profiler.queue;

import com.alibaba.profiler.Handler;
import com.alibaba.profiler.Task;
import com.alibaba.profiler.exception.QueueException;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public abstract class AbstractQueue implements Task {
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
     * pop data
     *
     * @return data
     */
    public abstract MessageWrapper getByPop();

    /**
     * take data
     * @return data
     */
    public abstract MessageWrapper getByTake();

    /**
     * update meta info
     * delete all queue files which all has consumed
     */
    public abstract void confirmLastMessage();

    public String pop() throws QueueException {
        MessageWrapper messageWrapper = getByPop();
        if (null == messageWrapper){
            return null;
        }
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message pop from the queue.");
            return null;
        }

        return convertMessage(buff);
    }



    public String pop(Handler handler) throws QueueException {
        MessageWrapper messageWrapper = getByPop();
        if (null == messageWrapper){
            return null;
        }
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message pop from the queue.");
            return null;
        }

        String message;
        try {
            message = new String(buff, "UTF-8");
        } catch (Exception e) {
            confirmLastMessage();
            throw new QueueException("Message conversion error occurred. ", e);
        }
        handler.handerData(message);
        confirmLastMessage();
        return message;
    }

    public String take() throws QueueException {
        MessageWrapper messageWrapper = getByTake();
        if (null == messageWrapper){
            return null;
        }
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message take from the queue.");
            return null;
        }
        return convertMessage(buff);
    }

    public String take(Handler handler) throws QueueException {
        MessageWrapper messageWrapper = getByTake();
        if (null == messageWrapper){
            return null;
        }
        byte[] buff = messageWrapper.getContent();
        if (buff.length <= 0) {
            LogUtil.warn("Null message take from the queue.");
            return null;
        }

        String message;
        try {
            message = new String(buff, "UTF-8");
        } catch (Exception e) {
            confirmLastMessage();
            throw new QueueException("Message type conversion error occurred. ", e);
        }
        handler.handerData(message);
        confirmLastMessage();
        return message;
    }

    private String convertMessage(byte[] buff) throws QueueException {
        String message;
        try {
            message = new String(buff, "UTF-8");
            confirmLastMessage();
            return message;
        } catch (Exception e) {
            confirmLastMessage();
            throw new QueueException("Message conversion error occurred. ", e);
        }
    }

    public void shutdown() {
        stopped = true;
        stop();
    }

    public void openQueue() {
        start();
    }

}
