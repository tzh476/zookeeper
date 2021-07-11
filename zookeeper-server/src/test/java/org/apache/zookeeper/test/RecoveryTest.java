package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class RecoveryTest extends ZKTestCase implements Watcher {
    protected static final Logger LOG = LoggerFactory.getLogger(RecoveryTest.class);

    private static final String HOSTPORT =
        "127.0.0.1:" + PortAssignment.unique();

    private volatile CountDownLatch startSignal;

    
    @Test
    public void testRecovery() throws Exception {
        File tmpDir = ClientBase.createTmpDir();

        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        int oldSnapCount = SyncRequestProcessor.getSnapCount();
        SyncRequestProcessor.setSnapCount(1000);
        try {
            final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
            ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
            f.startup(zks);
            LOG.info("starting up the the server, waiting");

            Assert.assertTrue("waiting for server up",
                       ClientBase.waitForServerUp(HOSTPORT,
                                       CONNECTION_TIMEOUT));

            startSignal = new CountDownLatch(1);
            ZooKeeper zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, this);
            startSignal.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);
            String path;
            LOG.info("starting creating nodes");
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                zk.create(path,
                          (path + "!").getBytes(),
                          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    zk.create(subpath, (subpath + "!").getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        zk.create(subsubpath, (subsubpath + "!").getBytes(),
                                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
            }

            f.shutdown();
            zks.shutdown();
            Assert.assertTrue("waiting for server down",
                       ClientBase.waitForServerDown(HOSTPORT,
                                          CONNECTION_TIMEOUT));

            zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
            f = ServerCnxnFactory.createFactory(PORT, -1);

            startSignal = new CountDownLatch(1);

            f.startup(zks);

            Assert.assertTrue("waiting for server up",
                       ClientBase.waitForServerUp(HOSTPORT,
                                           CONNECTION_TIMEOUT));

            startSignal.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);

            Stat stat = new Stat();
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                LOG.info("Checking " + path);
                Assert.assertEquals(new String(zk.getData(path, false, stat)), path
                        + "!");
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    Assert.assertEquals(new String(zk.getData(subpath, false, stat)),
                            subpath + "!");
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        Assert.assertEquals(new String(zk.getData(subsubpath, false,
                                stat)), subsubpath + "!");
                    }
                }
            }
            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                       ClientBase.waitForServerDown(HOSTPORT,
                                          ClientBase.CONNECTION_TIMEOUT));

            zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
            f = ServerCnxnFactory.createFactory(PORT, -1);

            startSignal = new CountDownLatch(1);

            f.startup(zks);

            Assert.assertTrue("waiting for server up",
                       ClientBase.waitForServerUp(HOSTPORT,
                               CONNECTION_TIMEOUT));

            startSignal.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertTrue("count == 0", startSignal.getCount() == 0);

            stat = new Stat();
            LOG.info("Check 2");
            for (int i = 0; i < 10; i++) {
                path = "/" + i;
                Assert.assertEquals(new String(zk.getData(path, false, stat)),
                             path + "!");
                for (int j = 0; j < 10; j++) {
                    String subpath = path + "/" + j;
                    Assert.assertEquals(new String(zk.getData(subpath, false, stat)),
                            subpath + "!");
                    for (int k = 0; k < 20; k++) {
                        String subsubpath = subpath + "/" + k;
                        Assert.assertEquals(new String(zk.getData(subsubpath, false,
                                stat)), subsubpath + "!");
                    }
                }
            }
            zk.close();

            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                       ClientBase.waitForServerDown(HOSTPORT,
                                                    CONNECTION_TIMEOUT));
        } finally {
            SyncRequestProcessor.setSnapCount(oldSnapCount);
        }
    }

    
    public void process(WatchedEvent event) {
        LOG.info("Event:" + event.getState() + " " + event.getType() + " " + event.getPath());
        if (event.getState() == KeeperState.SyncConnected
                && startSignal != null && startSignal.getCount() > 0)
        {
            startSignal.countDown();
        }
    }
}
