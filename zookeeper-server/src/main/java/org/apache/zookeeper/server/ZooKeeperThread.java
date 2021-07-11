package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZooKeeperThread extends Thread {

    private static final Logger LOG = LoggerFactory
            .getLogger(ZooKeeperThread.class);

    private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            handleException(t.getName(), e);
        }
    };

    public ZooKeeperThread(String threadName) {
        super(threadName);
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    
    protected void handleException(String thName, Throwable e) {
        LOG.warn("Exception occurred from thread {}", thName, e);
    }
}
