package com.alibaba.profiler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.profiler.exception.QueueException;
import com.alibaba.profiler.queue.PermanentQueue;

/**
 * Description:test
 *
 * @author wxy
 *         create 2017-05-14 下午10:31
 */

public class QueueTest {
    public static void main(String[] args) {
        /*File dir = new File("/meta");
        if (!dir.exists() && !dir.mkdirs()) {
            throw new RuntimeException("Cannot create meta directory: ");
        }*/
        ExecutorService producersPool = Executors.newFixedThreadPool(10);
        ExecutorService consumersPool = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            producersPool.submit(new Runnable() {
                @Override
                public void run() {
                    Long start = System.currentTimeMillis();
                    int sum = 0;
                    for (int j = 0; j < 100000; j++) {
                        try {
                            /*PermanentQueue.getInstance().offer("test",
                                String.valueOf(Thread.currentThread().getName() + "-" + j + ","));
                            PermanentQueue.getInstance().offer("test1",
                                String.valueOf(Thread.currentThread() + "-" + j + ","));*/
                            PermanentQueue.offer("test10", String.valueOf(j));
                            // System.out.println(PermanentQueue.getInstance().pop("test3"));
                            //sum = sum + Integer.valueOf(PermanentQueue.take("test10"));
                            //System.out.println("sum == " + sum);
                        } catch (QueueException e) {
                            System.out.println(e.getMessage());
                        } catch (Exception e){
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.println("sum == " + sum + " ,1000000 data write take == " + (
                        System.currentTimeMillis() - start));
                }
            });
        }

    }
}
