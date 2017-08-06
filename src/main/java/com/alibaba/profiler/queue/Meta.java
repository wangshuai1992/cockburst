package com.alibaba.profiler.queue;

import com.alibaba.profiler.util.CheckSum;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-17
 * Time: ÏÂÎç5:54
 * To change this template use File | Settings | File Templates.
 */
public class Meta {
    private volatile int readPos;
    private volatile long checkSum;
    private volatile String fileName;

    public int getReadPos() {
        return readPos;
    }

    public long getCheckSum() {
        return checkSum;
    }

    public String getFileName() {
        return fileName;
    }

    public static String concat(int readPos, String fileName) {
        if (fileName == null) {
            return String.valueOf(readPos);
        }
        return readPos + fileName;
    }

    public void set(int readPos, String fileName) {
        this.readPos = readPos;
        this.fileName = fileName;
        this.checkSum = CheckSum.adler32CheckSum(concat(readPos, fileName));
    }
}
