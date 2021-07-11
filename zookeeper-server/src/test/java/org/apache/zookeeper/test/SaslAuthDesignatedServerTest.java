package org.apache.zookeeper.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.apache.zookeeper.JaasConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class SaslAuthDesignatedServerTest extends ClientBase {
    public static int AUTHENTICATION_TIMEOUT = 30000;

    static {
        System.setProperty("zookeeper.authProvider.1","org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY, "MyZookeeperServer");

        JaasConfiguration conf = new JaasConfiguration();

        
        conf.addSection("Server", "org.apache.zookeeper.server.auth.DigestLoginModule",
                        "user_myuser", "wrongpassword");

        conf.addSection("MyZookeeperServer", "org.apache.zookeeper.server.auth.DigestLoginModule",
                        "user_myuser", "mypassword");

        conf.addSection("Client", "org.apache.zookeeper.server.auth.DigestLoginModule",
                        "username", "myuser", "password", "mypassword");

        javax.security.auth.login.Configuration.setConfiguration(conf);
    }

    private AtomicInteger authFailed = new AtomicInteger(0);

    private class MyWatcher extends CountdownWatcher {
        volatile CountDownLatch authCompleted;

        @Override
        synchronized public void reset() {
            authCompleted = new CountDownLatch(1);
            super.reset();
        }

        @Override
        public synchronized void process(WatchedEvent event) {
            if (event.getState() == KeeperState.AuthFailed) {
                authFailed.incrementAndGet();
                authCompleted.countDown();
            } else if (event.getState() == KeeperState.SaslAuthenticated) {
                authCompleted.countDown();
            } else {
                super.process(event);
            }
        }
    }

    @Test
    public void testAuth() throws Exception {
        MyWatcher watcher = new MyWatcher();
        ZooKeeper zk = createClient(watcher);
        watcher.authCompleted.await(AUTHENTICATION_TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(authFailed.get(), 0);

        try {
            zk.create("/path1", null, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          Assert.fail("test failed :" + e);
        }
        finally {
            zk.close();
        }
    }
}
