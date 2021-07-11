package org.apache.zookeeper.test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumTest.class);
    public static final long CONNECTION_TIMEOUT = ClientTest.CONNECTION_TIMEOUT;

    private final QuorumBase qb = new QuorumBase();
    private final ClientTest ct = new ClientTest();
    private QuorumUtil qu;

    @Before
    public void setUp() throws Exception {
        qb.setUp();
        ct.hostPort = qb.hostPort;
        ct.setUpAll();
    }

    @After
    public void tearDown() throws Exception {
        ct.tearDownAll();
        qb.tearDown();
        if (qu != null) {
            qu.tearDown();
        }
    }

    @Test
    public void testDeleteWithChildren() throws Exception {
        ct.testDeleteWithChildren();
    }

    @Test
    public void testPing() throws Exception {
        ct.testPing();
    }

    @Test
    public void testSequentialNodeNames()
        throws IOException, InterruptedException, KeeperException
    {
        ct.testSequentialNodeNames();
    }

    @Test
    public void testACLs() throws Exception {
        ct.testACLs();
    }

    @Test
    public void testClientwithoutWatcherObj() throws IOException,
            InterruptedException, KeeperException
    {
        ct.testClientwithoutWatcherObj();
    }

    @Test
    public void testClientWithWatcherObj() throws IOException,
            InterruptedException, KeeperException
    {
        ct.testClientWithWatcherObj();
    }

    @Test
    public void testGetView() {
        Assert.assertEquals(5,qb.s1.getView().size());
        Assert.assertEquals(5,qb.s2.getView().size());
        Assert.assertEquals(5,qb.s3.getView().size());
        Assert.assertEquals(5,qb.s4.getView().size());
        Assert.assertEquals(5,qb.s5.getView().size());
    }

    @Test
    public void testViewContains() {
                Assert.assertTrue(qb.s1.viewContains(qb.s1.getId()));

                Assert.assertTrue(qb.s1.viewContains(qb.s2.getId()));

                Assert.assertFalse(qb.s1.viewContains(-1L));
    }

    volatile int counter = 0;
    volatile int errors = 0;
    @Test
    public void testLeaderShutdown() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = new DisconnectableZooKeeper(qb.hostPort, ClientBase.CONNECTION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent event) {
        }});
        zk.create("/blah", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/blah/blah", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Leader leader = qb.s1.leader;
        if (leader == null) leader = qb.s2.leader;
        if (leader == null) leader = qb.s3.leader;
        if (leader == null) leader = qb.s4.leader;
        if (leader == null) leader = qb.s5.leader;
        Assert.assertNotNull(leader);
        for(int i = 0; i < 5000; i++) {
            zk.setData("/blah/blah", new byte[0], -1, new AsyncCallback.StatCallback() {
                public void processResult(int rc, String path, Object ctx,
                        Stat stat) {
                    counter++;
                    if (rc != 0) {
                        errors++;
                    }
                }
            }, null);
        }
        for(LearnerHandler f : leader.getForwardingFollowers()) {
            f.getSocket().shutdownInput();
        }
        for(int i = 0; i < 5000; i++) {
            zk.setData("/blah/blah", new byte[0], -1, new AsyncCallback.StatCallback() {
                public void processResult(int rc, String path, Object ctx,
                        Stat stat) {
                    counter++;
                    if (rc != 0) {
                        errors++;
                    }
                }
            }, null);
        }
                Assert.assertTrue(qb.s1.isAlive());
        Assert.assertTrue(qb.s2.isAlive());
        Assert.assertTrue(qb.s3.isAlive());
        Assert.assertTrue(qb.s4.isAlive());
        Assert.assertTrue(qb.s5.isAlive());
        zk.close();
    }

    @Test
    public void testMultipleWatcherObjs() throws IOException,
            InterruptedException, KeeperException
    {
        ct.testMutipleWatcherObjs();
    }

    
    @Test
    public void testSessionMoved() throws Exception {
        String hostPorts[] = qb.hostPort.split(",");
        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(hostPorts[0],
                ClientBase.CONNECTION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/sessionMoveTest", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                for(int i = 0; i < hostPorts.length*2; i++) {
            zk.dontReconnect();
                        DisconnectableZooKeeper zknew =
                new DisconnectableZooKeeper(hostPorts[(i+1)%hostPorts.length],
                    ClientBase.CONNECTION_TIMEOUT,
                    new Watcher() {public void process(WatchedEvent event) {
                    }},
                    zk.getSessionId(),
                    zk.getSessionPasswd());
            zknew.setData("/", new byte[1], -1);
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
            try {
                zk.setData("/", new byte[1], -1);
                Assert.fail("Should have lost the connection");
            } catch(KeeperException.ConnectionLossException e) {
            }
            zk = zknew;
        }
        zk.close();
    }

    private static class DiscoWatcher implements Watcher {
        volatile boolean zkDisco = false;
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected) {
                zkDisco = true;
            }
        }
    }

    
    @Test
    @Ignore
    public void testSessionMove() throws Exception {
        String hps[] = qb.hostPort.split(",");
        DiscoWatcher oldWatcher = new DiscoWatcher();
        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(hps[0],
                ClientBase.CONNECTION_TIMEOUT, oldWatcher);
        zk.create("/t1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.dontReconnect();
                DiscoWatcher watcher = new DiscoWatcher();
        DisconnectableZooKeeper zknew = new DisconnectableZooKeeper(hps[1],
                ClientBase.CONNECTION_TIMEOUT, watcher, zk.getSessionId(),
                zk.getSessionPasswd());
        zknew.create("/t2", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        try {
            zk.create("/t3", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Assert.fail("Should have lost the connection");
        } catch(KeeperException.ConnectionLossException e) {
                        for (int i = 0; i < 30; i++) {
                if (oldWatcher.zkDisco) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(oldWatcher.zkDisco);
        }

        ArrayList<ZooKeeper> toClose = new ArrayList<ZooKeeper>();
        toClose.add(zknew);
                for(int i = 0; i < 10; i++) {
            zknew.dontReconnect();
            zknew = new DisconnectableZooKeeper(hps[1],
                    ClientBase.CONNECTION_TIMEOUT, new DiscoWatcher(),
                    zk.getSessionId(), zk.getSessionPasswd());
            toClose.add(zknew);
            zknew.create("/t-"+i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        for (ZooKeeper z: toClose) {
            z.close();
        }
        zk.close();
    }

    
    @Test
    public void testFollowersStartAfterLeader() throws Exception {
        qu = new QuorumUtil(1);
        CountdownWatcher watcher = new CountdownWatcher();
        qu.startQuorum();

        int index = 1;
        while(qu.getPeer(index).peer.leader == null)
            index++;

                qu.shutdown(index);
        
                qu.start(index);
        
                        ZooKeeper zk = new ZooKeeper(
                "127.0.0.1:" + qu.getPeer((index == 1)?2:1).peer.getClientPort(),
                ClientBase.CONNECTION_TIMEOUT, watcher);

        try{
            watcher.waitForConnected(CONNECTION_TIMEOUT);      
        } catch(TimeoutException e) {
            Assert.fail("client could not connect to reestablished quorum: giving up after 30+ seconds.");
        }

        zk.close();
    }

    
    
    @Test
    public void testMultiToFollower() throws Exception {
        qu = new QuorumUtil(1);
        CountdownWatcher watcher = new CountdownWatcher();
        qu.startQuorum();

        int index = 1;
        while(qu.getPeer(index).peer.leader == null)
            index++;

        ZooKeeper zk = new ZooKeeper(
                "127.0.0.1:" + qu.getPeer((index == 1)?2:1).peer.getClientPort(),
                ClientBase.CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        zk.multi(Arrays.asList(
                Op.create("/multi0", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create("/multi1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create("/multi2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                ));
        zk.getData("/multi0", false, null);
        zk.getData("/multi1", false, null);
        zk.getData("/multi2", false, null);

        zk.close();
    }
}
