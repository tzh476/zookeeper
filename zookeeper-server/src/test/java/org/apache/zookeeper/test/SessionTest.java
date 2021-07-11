package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(SessionTest.class);

    private static final String HOSTPORT = "127.0.0.1:" +
            PortAssignment.unique();

    private ServerCnxnFactory serverFactory;
    private ZooKeeperServer zs;

    private CountDownLatch startSignal;

    File tmpDir;

    private final int TICK_TIME = 3000;

    @Before
    public void setUp() throws Exception {
        if (tmpDir == null) {
            tmpDir = ClientBase.createTmpDir();
        }

        ClientBase.setupTestEnv();
        zs = new ZooKeeperServer(tmpDir, tmpDir, TICK_TIME);

        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        serverFactory = ServerCnxnFactory.createFactory(PORT, -1);
        serverFactory.startup(zs);

        Assert.assertTrue("waiting for server up",
                   ClientBase.waitForServerUp(HOSTPORT,
                                              CONNECTION_TIMEOUT));
    }

    @After
    public void tearDown() throws Exception {
        serverFactory.shutdown();
        zs.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown(HOSTPORT,
                                                CONNECTION_TIMEOUT));
    }

    private static class CountdownWatcher implements Watcher {
        volatile CountDownLatch clientConnected = new CountDownLatch(1);

        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                clientConnected.countDown();
            }
        }
    }

    private DisconnectableZooKeeper createClient()
        throws IOException, InterruptedException
    {
        CountdownWatcher watcher = new CountdownWatcher();
        return createClient(CONNECTION_TIMEOUT, watcher);
    }

    private DisconnectableZooKeeper createClient(int timeout)
        throws IOException, InterruptedException
    {
        CountdownWatcher watcher = new CountdownWatcher();
        return createClient(timeout, watcher);
    }

    private DisconnectableZooKeeper createClient(int timeout,
            CountdownWatcher watcher)
        throws IOException, InterruptedException
    {
        DisconnectableZooKeeper zk =
                new DisconnectableZooKeeper(HOSTPORT, timeout, watcher);
        if(!watcher.clientConnected.await(timeout, TimeUnit.MILLISECONDS)) {
            Assert.fail("Unable to connect to server");
        }

        return zk;
    }


    private class MyWatcher implements Watcher {
        private String name;
        public MyWatcher(String name) {
            this.name = name;
        }
        public void process(WatchedEvent event) {
            LOG.info(name + " event:" + event.getState() + " "
                    + event.getType() + " " + event.getPath());
            if (event.getState() == KeeperState.SyncConnected
                    && startSignal != null && startSignal.getCount() > 0)
            {
                startSignal.countDown();
            }
        }
    }

    
    @Test
    public void testSession()
        throws IOException, InterruptedException, KeeperException
    {
        DisconnectableZooKeeper zk = createClient();
        zk.create("/e", new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
        LOG.info("zk with session id 0x" + Long.toHexString(zk.getSessionId())
                + " was destroyed!");

                                zk.disconnect();

        Stat stat = new Stat();
        startSignal = new CountDownLatch(1);
        zk = new DisconnectableZooKeeper(HOSTPORT, CONNECTION_TIMEOUT,
                new MyWatcher("testSession"), zk.getSessionId(),
                zk.getSessionPasswd());
        startSignal.await();

        LOG.info("zk with session id 0x" + Long.toHexString(zk.getSessionId())
                 + " was created!");
        zk.getData("/e", false, stat);
        LOG.info("After get data /e");
        zk.close();

        zk = createClient();
        Assert.assertEquals(null, zk.exists("/e", false));
        LOG.info("before close zk with session id 0x"
                + Long.toHexString(zk.getSessionId()) + "!");
        zk.close();
        try {
            zk.getData("/e", false, stat);
            Assert.fail("Should have received a SessionExpiredException");
        } catch(KeeperException.SessionExpiredException e) {}

        AsyncCallback.DataCallback cb = new AsyncCallback.DataCallback() {
            String status = "not done";
            public void processResult(int rc, String p, Object c, byte[] b, Stat s) {
                synchronized(this) { status = KeeperException.Code.get(rc).toString(); this.notify(); }
            }
           public String toString() { return status; }
        };
        zk.getData("/e", false, cb, null);
        synchronized(cb) {
            if (cb.toString().equals("not done")) {
                cb.wait(1000);
            }
        }
        Assert.assertEquals(KeeperException.Code.SESSIONEXPIRED.toString(), cb.toString());
    }

    
    @Test
    public void testSessionMove() throws Exception {
        String hostPorts[] = HOSTPORT.split(",");
        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(hostPorts[0],
                CONNECTION_TIMEOUT, new MyWatcher("0"));
        zk.create("/sessionMoveTest", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
                for(int i = 0; i < hostPorts.length*2; i++) {
            zk.dontReconnect();
                        DisconnectableZooKeeper zknew = new DisconnectableZooKeeper(
                    hostPorts[(i+1)%hostPorts.length],
                    CONNECTION_TIMEOUT,
                    new MyWatcher(Integer.toString(i+1)),
                    zk.getSessionId(),
                    zk.getSessionPasswd());
            final int result[] = new int[1];
            result[0] = Integer.MAX_VALUE;
            zknew.sync("/", new AsyncCallback.VoidCallback() {
                    public void processResult(int rc, String path, Object ctx) {
                        synchronized(result) { result[0] = rc; result.notify(); }
                    }
                }, null);
            synchronized(result) {
                if(result[0] == Integer.MAX_VALUE) {
                    result.wait(5000);
                }
            }
            LOG.info(hostPorts[(i+1)%hostPorts.length] + " Sync returned " + result[0]);
            Assert.assertTrue(result[0] == KeeperException.Code.OK.intValue());
            zknew.setData("/", new byte[1], -1);
            try {
                zk.setData("/", new byte[1], -1);
                Assert.fail("Should have lost the connection");
            } catch(KeeperException.ConnectionLossException e) {
                LOG.info("Got connection loss exception as expected");
            }
                        zk = zknew;
        }
        zk.close();
    }
    
    @Test
    public void testSessionStateNoDupStateReporting()
        throws IOException, InterruptedException, KeeperException
    {
        final int TIMEOUT = 3000;
        DupWatcher watcher = new DupWatcher();
        ZooKeeper zk = createClient(TIMEOUT, watcher);

                serverFactory.shutdown();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
                    }

                                        Assert.assertEquals(2, watcher.states.size());

        zk.close();
    }

    
    @Test
    public void testSessionTimeoutAccess() throws Exception {
                DisconnectableZooKeeper zk = createClient(TICK_TIME * 4);
        Assert.assertEquals(TICK_TIME * 4, zk.getSessionTimeout());
                LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());

                zk = createClient(TICK_TIME);
        Assert.assertEquals(TICK_TIME * 2, zk.getSessionTimeout());
        LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());

                zk = createClient(TICK_TIME * 30);
        Assert.assertEquals(TICK_TIME * 20, zk.getSessionTimeout());
        LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());
    }

    private class DupWatcher extends CountdownWatcher {
        public LinkedList<WatchedEvent> states = new LinkedList<WatchedEvent>();
        public void process(WatchedEvent event) {
            super.process(event);
            if (event.getType() == EventType.None) {
                states.add(event);
            }
        }
    }

    @Test
    public void testMinMaxSessionTimeout() throws Exception {
                final int MINSESS = 20000;
        final int MAXSESS = 240000;
        {
            ZooKeeperServer zs = ClientBase.getServer(serverFactory);
            zs.setMinSessionTimeout(MINSESS);
            zs.setMaxSessionTimeout(MAXSESS);
        }

                int timeout = 120000;
        DisconnectableZooKeeper zk = createClient(timeout);
        Assert.assertEquals(timeout, zk.getSessionTimeout());
                LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());

                zk = createClient(MINSESS/2);
        Assert.assertEquals(MINSESS, zk.getSessionTimeout());
        LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());

                zk = createClient(MAXSESS * 2);
        Assert.assertEquals(MAXSESS, zk.getSessionTimeout());
        LOG.info(zk.toString());
        zk.close();
        LOG.info(zk.toString());
    }
}
