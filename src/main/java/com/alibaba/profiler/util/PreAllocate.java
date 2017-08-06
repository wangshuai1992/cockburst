package com.alibaba.profiler.util;

import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-23
 * Time: ÏÂÎç6:25
 * To change this template use File | Settings | File Templates.
 */
public class PreAllocate {
    private ByteBuffer bf;
    private final boolean direct;
    private final double factor;

    public PreAllocate() {
        this(1.5);
    }

    public PreAllocate(double factor) {
        this(factor, false);
    }

    public PreAllocate(double factor, boolean direct) {
        this.factor = factor;
        this.direct = direct;
    }

    public ByteBuffer allocate(int len) {
        if (bf == null || bf.capacity() < len) {
            int cap = (int) (len * factor);
            if (direct) {
                bf = ByteBuffer.allocateDirect(cap);
            } else {
                bf = ByteBuffer.allocate(cap);
            }
        }
        bf.clear();
        return bf;
    }
}
