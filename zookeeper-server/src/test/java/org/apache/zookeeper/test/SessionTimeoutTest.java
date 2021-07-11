package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SessionTimeoutTest extends ClientBase {
    protected static final Logger LOG = LoggerFactory.getLogger(SessionTimeoutTest.class);

    private TestableZooKeeper zk;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        zk = createClient();
    }

    @Test
    public void testSessionExpiration() throws InterruptedException,
            KeeperException {
        final CountDownLatch expirationLatch = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if ( event.getState() == Event.KeeperState.Expired ) {
                    expirationLatch.countDown();
                }
            }
        };
        zk.exists("/foo", watcher);

        zk.getTestable().injectSessionExpiration();
        Assert.assertTrue(expirationLatch.await(5, TimeUnit.SECONDS));

        boolean gotException = false;
        try {
            zk.exists("/foo", false);
            Assert.fail("Should have thrown a SessionExpiredException");
        } catch (KeeperException.SessionExpiredException e) {
                        gotException = true;
        }
        Assert.assertTrue(gotException);
    }

    
    @Test
    public void testSessionDisconnect() throws KeeperException, InterruptedException, IOException {
        zk.create("/sdisconnect", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        assertNotNull("Ephemeral node has not been created", zk.exists("/sdisconnect", null));

        zk.close();

        zk = createClient();
        assertNull("Ephemeral node shouldn't exist after client disconnect", zk.exists("/sdisconnect", null));
    }

    
    @Test
    public void testSessionRestore() throws KeeperException, InterruptedException, IOException {
        zk.create("/srestore", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        assertNotNull("Ephemeral node has not been created", zk.exists("/srestore", null));

        zk.disconnect();
        zk.close();

        zk = createClient();
        assertNotNull("Ephemeral node should be present when session is restored", zk.exists("/srestore", null));
    }

    
    @Test
    public void testSessionSurviveServerRestart() throws Exception {
        zk.create("/sdeath", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        assertNotNull("Ephemeral node has not been created", zk.exists("/sdeath", null));

        zk.disconnect();
        stopServer();
        startServer();
        zk = createClient();

        assertNotNull("Ephemeral node should be present when server restarted", zk.exists("/sdeath", null));
    }
}
