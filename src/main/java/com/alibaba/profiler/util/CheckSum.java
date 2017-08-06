package com.alibaba.profiler.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.Adler32;

/**
 * Created by IntelliJ IDEA.
 * User: caojiadong
 * Date: 13-4-19
 * Time: обнГ6:32
 * To change this template use File | Settings | File Templates.
 */
public class CheckSum {
    public static long adler32CheckSum(long src) {
        long cs = 0;
        Throwable t = null;
        for (int i = 0; i < 3; i++) {
            try {
                cs = calcCheckSum(String.valueOf(src));
                return cs;
            } catch (IOException e) {
                t = e;
                continue;
            }
        }
        throw new RuntimeException("Cannot calc check sum: " + src, t);
    }

    public static long adler32CheckSum(String src) {
        long cs = 0;
        Throwable t = null;
        for (int i = 0; i < 3; i++) {
            try {
                cs = calcCheckSum(src);
                return cs;
            } catch (IOException e) {
                t = e;
                continue;
            }
        }
        throw new RuntimeException("Cannot not calc check sum: " + src, t);
    }

    private static long calcCheckSum(String src) throws IOException {
        byte[] sb = src.getBytes(Charset.forName("UTF-8"));
        Adler32 checksum = new Adler32();
        checksum.update(sb);
        return checksum.getValue();
    }
}
