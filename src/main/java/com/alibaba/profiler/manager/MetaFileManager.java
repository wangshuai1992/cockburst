package com.alibaba.profiler.manager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import com.alibaba.profiler.config.QueueConfig;
import com.alibaba.profiler.queue.Meta;
import com.alibaba.profiler.util.CheckSum;
import com.alibaba.profiler.util.LogUtil;

/**
 * @author wxy on 16/6/4.
 */
public class MetaFileManager {
    private final Meta meta;
    private final String metaPath;
    private MappedByteBuffer mbb;
    private final static String META_FILE_SUFFIX = ".meta";
    private final static int MAX_META_LEN = 2048;
    private RandomAccessFile metaFile;
    private final String metaFileName;

    public MetaFileManager(String queueName) {
        meta = new Meta();
        metaPath = QueueConfig.getInstance().getMetaPath() + "/";
        metaFileName = queueName + META_FILE_SUFFIX;
        buildMeta();
    }

    private void buildMeta() {
        File dir = new File(metaPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new RuntimeException("Cannot create meta directory == " + metaPath);
        }

        try {
            metaFile = new RandomAccessFile(metaPath + metaFileName, "rw");
            mbb = metaFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MAX_META_LEN);
        } catch (Exception e) {
            throw new RuntimeException("Create meta file failed", e);
        }

        loadMetaFromFile();
    }

    private void loadMetaFromFile() {
        mbb.position(0);
        int pos = (int) mbb.getLong();
        int len = mbb.getInt();
        if (len > MAX_META_LEN - 20) {
            LogUtil.error("Incorrect meta content, reset it.");
            meta.set(0, null);
            return;
        }
        byte[] dst = new byte[len];
        try {
            mbb.get(dst);
        } catch (BufferUnderflowException e) {
            LogUtil.error("Incorrect meta content, reset it.");
            meta.set(0, null);
            return;
        }
        long ck = mbb.getLong();
        String name = new String(dst, Charset.forName("UTF-8"));
        checkValidateMeta(pos, name, ck);
    }

    private void checkValidateMeta(int pos, String fileName, long ck) {
        long ck2 = CheckSum.adler32CheckSum(Meta.concat(pos, fileName));
        if (ck2 != ck) {
            LogUtil.warn("Incorrect check sum value " + ck + " != " + ck2 + ", reset it.");
            meta.set(0, null);
        } else {
            meta.set(pos, fileName);
        }
    }

    public Meta get() {
        return meta;
    }

    public synchronized void update(int pos, String fileName) {
        meta.set(pos, fileName);
        mbb.position(0);

        byte[] bytes = fileName.getBytes(Charset.forName("UTF-8"));
        mbb.putLong((long) meta.getReadPos());
        mbb.putInt(bytes.length);
        mbb.put(bytes);
        mbb.putLong(meta.getCheckSum());
    }

    public synchronized void close() {
        mbb.force();
        mbb = null;
        try {
            metaFile.close();
        } catch (IOException e) {
            LogUtil.error("Close metaFile error. " + e);
        }
    }
}
