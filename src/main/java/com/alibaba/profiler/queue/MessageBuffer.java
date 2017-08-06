package com.alibaba.profiler.queue;

import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-23
 * Time: ÏÂÎç8:27
 * To change this template use File | Settings | File Templates.
 */
public class MessageBuffer {
    private ByteBuffer bf;
    private int limit;
    private boolean allocated;

    public MessageBuffer() {
        limit = 0;
        bf = null;
        allocated = false;
    }

    public void allocate(int len) {
        allocated = true;
        limit = len;
        bf = ByteBuffer.allocateDirect(len);
    }

    public ByteBuffer get() {
        return bf;
    }

    public int position() {
        return bf.position();
    }

    public void put(byte b) {
        bf.put(b);
    }

    public void put(ByteBuffer src) {
        bf.put(src);
    }

    public int remaining() {
        return limit - bf.position();
    }

    public void flip() {
        bf.flip();
    }

    public void clear() {
        allocated = false;
        bf.clear();
    }

    public int getLimit() {
        return limit;
    }

    public boolean isAllocated() {
        return allocated;
    }

    public void setAllocated(boolean allocated) {
        this.allocated = allocated;
    }
}
