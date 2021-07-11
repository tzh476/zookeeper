package org.apache.zookeeper.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperThreadTest extends ZKTestCase {
    private CountDownLatch runningLatch = new CountDownLatch(1);

    public class MyThread extends ZooKeeperThread {

        public MyThread(String threadName) {
            super(threadName);
        }

        public void run() {
            throw new Error();
        }

        @Override
        protected void handleException(String thName, Throwable e) {
            runningLatch.countDown();
        }
    }

    public class MyCriticalThread extends ZooKeeperCriticalThread {

        public MyCriticalThread(String threadName) {
            super(threadName, new ZooKeeperServerListener() {

                @Override
                public void notifyStopping(String threadName, int erroCode) {

                }
            });
        }

        public void run() {
            throw new Error();
        }

        @Override
        protected void handleException(String thName, Throwable e) {
            runningLatch.countDown();
        }
    }

    
    @Test(timeout = 30000)
    public void testUncaughtException() throws Exception {
        MyThread t1 = new MyThread("Test-Thread");
        t1.start();
        Assert.assertTrue("Uncaught exception is not properly handled.",
                runningLatch.await(10000, TimeUnit.MILLISECONDS));

        runningLatch = new CountDownLatch(1);
        MyCriticalThread t2 = new MyCriticalThread("Test-Critical-Thread");
        t2.start();
        Assert.assertTrue("Uncaught exception is not properly handled.",
                runningLatch.await(10000, TimeUnit.MILLISECONDS));
    }
}
