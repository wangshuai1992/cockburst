package com.alibaba.profiler;

import java.io.File;

import com.alibaba.profiler.exception.FailedException;
import com.alibaba.profiler.queue.PermanentQueue;

/**
 * Description:test
 *
 * @author wxy
 * create 2017-05-14 下午10:31
 */

public class QueueTest {
    public static void main(String[] args) {
        /*File dir = new File("/meta");
        if (!dir.exists() && !dir.mkdirs()) {
            throw new RuntimeException("Cannot create meta directory: ");
        }*/
        for (int i = 0;i<30;i++){
            try {
                //PermanentQueue.getInstance().offer("test",String.valueOf("a"+i));
                System.out.println(PermanentQueue.getInstance().pop("test"));
            } catch (FailedException e) {
                e.printStackTrace();
            }
        }


    }
}
