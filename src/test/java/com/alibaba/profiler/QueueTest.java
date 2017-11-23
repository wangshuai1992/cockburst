package com.alibaba.profiler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.profiler.exception.QueueException;
import com.alibaba.profiler.queue.PermanentQueue;

/**
 * Description:test
 *
 * @author wxy
 * create 2017-05-14 下午10:31
 */

public class QueueTest {
    private String queueName;
    private int poolSize;
    private int dataNums;


    public static void main(String[] args) {
        QueueTest queueTest = new QueueTest("0923_7",10,100);
        //queueTest.productTest();
        queueTest.consumerTest();
    }

    private void productTest() {
        ExecutorService productPool = Executors.newFixedThreadPool(poolSize);
        final CountDownLatch countDownLatch = new CountDownLatch(poolSize);
        final AtomicInteger allSum = new AtomicInteger();
        final Long start = System.currentTimeMillis();
        for (int i = 0; i < poolSize; i++) {
            productPool.submit(new Runnable() {
                @Override
                public void run() {
                    int sum = 0;
                    for (int j = 0; j < dataNums; j++) {
                        try {
                            PermanentQueue.offer(queueName, String.valueOf(j));
                            sum = sum + j;
                        } catch (QueueException e) {
                            System.out.println(e.getMessage());
                        }

                    }
                    countDownLatch.countDown();
                    allSum.addAndGet(sum);
                    System.out.println(
                        "Thread: " + Thread.currentThread().getName() + " 100000 data write take == " + (
                            System.currentTimeMillis() - start));
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Write take time == " + (System.currentTimeMillis() - start));
        System.out.println("Write sum == " + allSum.get());
    }

    private void consumerTest() {
        ExecutorService consumersPool = Executors.newFixedThreadPool(poolSize);
        final CountDownLatch countDownLatch = new CountDownLatch(poolSize);
        final AtomicInteger allSum = new AtomicInteger();
        final Long start = System.currentTimeMillis();
        for (int i = 0; i < poolSize; i++) {
            consumersPool.submit(new Runnable() {
                @Override
                public void run() {
                    int sum = 0;
                    int count = 0;
                    for (int j = 0; j < dataNums; j++) {
                        try {
                            String value = PermanentQueue.take(queueName);
                            if (null != value && !"".equals(value)) {
                                count++;
                                sum = sum + Integer.valueOf(value);
                            }
                        } catch (QueueException e) {
                            System.out.println(e.getMessage());
                        }

                    }

                    countDownLatch.countDown();
                    allSum.addAndGet(sum);
                    System.out.println(
                        "Thread: " + Thread.currentThread().getName() + " sum == " + sum + " count == " + count
                            + " take time == " + (System.currentTimeMillis() - start));
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Read take time == " + (System.currentTimeMillis() - start));
        System.out.println("Read sum == " + allSum);
    }

    public void testOffer() throws QueueException {
        PermanentQueue.offer("category", "data1");
    }

    public void testPop() throws QueueException {
        PermanentQueue.pop("category");
    }

    public void testTake() throws QueueException {
        PermanentQueue.take("category");
    }


    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getDataNums() {
        return dataNums;
    }

    public void setDataNums(int dataNums) {
        this.dataNums = dataNums;
    }

    public QueueTest(String queueName, int poolSize, int dataNums) {
        this.queueName = queueName;
        this.poolSize = poolSize;
        this.dataNums = dataNums;
    }


}
