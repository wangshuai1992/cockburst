package com.alibaba.profiler.queue;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-23
 * Time: ÏÂÎç8:20
 * To change this template use File | Settings | File Templates.
 */
public class MessageWrapper {
    private final byte[] content;
    private final boolean firstMessage;
    private final int endPos;
    private final String currentFile;
    private  String message;

    public MessageWrapper(byte[] content, boolean firstMessage, int endPos, String currentFile) {
        this.content = content;
        this.firstMessage = firstMessage;
        this.endPos = endPos;
        this.currentFile = currentFile;
    }

    public byte[] getContent() {
        return content;
    }

    public boolean isFirstMessage() {
        return firstMessage;
    }

    public int getEndPos() {
        return endPos;
    }

    public String getCurrentFile() {
        return currentFile;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
