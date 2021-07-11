package org.apache.zookeeper.server;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.server.ZooKeeperServer.State;


class ZooKeeperServerShutdownHandler {
    private final CountDownLatch shutdownLatch;

    ZooKeeperServerShutdownHandler(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
    }

    
    void handle(State state) {
        if (state == State.ERROR || state == State.SHUTDOWN) {
            shutdownLatch.countDown();
        }
    }
}
