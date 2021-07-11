package org.apache.zookeeper.server;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.apache.zookeeper.test.ClientTest;
import org.apache.zookeeper.test.QuorumUtil;
import org.apache.zookeeper.test.QuorumUtil.PeerStruct;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZxidRolloverTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ZxidRolloverTest.class);

    private QuorumUtil qu;
    private ZooKeeperServer zksLeader;
    private ZooKeeper[] zkClients = new ZooKeeper[3];
    private CountdownWatcher[] zkClientWatchers = new CountdownWatcher[3];
    private int idxLeader;
    private int idxFollower;
    
    private ZooKeeper getClient(int idx) {
        return zkClients[idx-1];
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");

                        SyncRequestProcessor.setSnapCount(7);

        qu = new QuorumUtil(1);
        startAll();

        for (int i = 0; i < zkClients.length; i++) {
            zkClientWatchers[i] = new CountdownWatcher();
            PeerStruct peer = qu.getPeer(i + 1);
            zkClients[i] = new ZooKeeper(
                    "127.0.0.1:" + peer.clientPort,
                    ClientTest.CONNECTION_TIMEOUT, zkClientWatchers[i]);
        }
        waitForClientsConnected();
    }
    
    private void waitForClientsConnected() throws Exception {
        for (int i = 0; i < zkClients.length; i++) {
            zkClientWatchers[i].waitForConnected(ClientTest.CONNECTION_TIMEOUT);
            zkClientWatchers[i].reset();
        }
    }

    
    private void checkClientsConnected() throws Exception {
        for (int i = 0; i < zkClients.length; i++) {
            checkClientConnected(i + 1);
        }
    }

    
    private void checkClientConnected(int idx) throws Exception {
        ZooKeeper zk = getClient(idx);
        if (zk == null) {
            return;
        }
        try {
            Assert.assertNull(zk.exists("/foofoofoo-connected", false));
        } catch (ConnectionLossException e) {
                                                                                                            PeerStruct peer = qu.getPeer(idx);
            Assert.assertTrue("Waiting for server down", ClientBase.waitForServerUp(
                    "127.0.0.1:" + peer.clientPort, ClientBase.CONNECTION_TIMEOUT));

            Assert.assertNull(zk.exists("/foofoofoo-connected", false));
        }
    }

    
    private void checkClientsDisconnected() throws Exception {
        for (int i = 0; i < zkClients.length; i++) {
            checkClientDisconnected(i + 1);
        }
    }

    
    private void checkClientDisconnected(int idx) throws Exception {
        ZooKeeper zk = getClient(idx);
        if (zk == null) {
            return;
        }
        try {
            Assert.assertNull(zk.exists("/foofoofoo-disconnected", false));
            Assert.fail("expected client to be disconnected");
        } catch (KeeperException e) {
                    }
    }

    private void startAll() throws Exception {
        qu.startAll();
        checkLeader();
                checkClientsConnected();
    }
    private void start(int idx) throws Exception {
        qu.start(idx);
        for (String hp : qu.getConnString().split(",")) {
            Assert.assertTrue("waiting for server up", ClientBase.waitForServerUp(hp,
                    ClientTest.CONNECTION_TIMEOUT));
        }

        checkLeader();
                checkClientsConnected();
    }

    private void checkLeader() {
        idxLeader = 1;
        while(qu.getPeer(idxLeader).peer.leader == null) {
            idxLeader++;
        }
        idxFollower = (idxLeader == 1 ? 2 : 1);

        zksLeader = qu.getPeer(idxLeader).peer.getActiveServer();
    }

    private void shutdownAll() throws Exception {
        qu.shutdownAll();
                checkClientsDisconnected();
    }
    
    private void shutdown(int idx) throws Exception {
        qu.shutdown(idx);

                PeerStruct peer = qu.getPeer(idx);
        Assert.assertTrue("Waiting for server down", ClientBase.waitForServerDown(
                "127.0.0.1:" + peer.clientPort, ClientBase.CONNECTION_TIMEOUT));

                                if (idx == idxLeader) {
            checkClientDisconnected(idx);
            try {
                checkClientsDisconnected();
            } catch (AssertionError e) {
                                            }
        } else {
            checkClientDisconnected(idx);
        }
    }

    
    private void adjustEpochNearEnd() {
        zksLeader.setZxid((zksLeader.getZxid() & 0xffffffff00000000L) | 0xfffffffcL);
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("tearDown starting");
        for (int i = 0; i < zkClients.length; i++) {
            zkClients[i].close();
        }
        qu.shutdownAll();
    }

    
    private int createNodes(ZooKeeper zk, int start, int count) throws Exception {
        LOG.info("Creating nodes {} thru {}", start, (start + count));
        int j = 0;
        try {
            for (int i = start; i < start + count; i++) {
                zk.create("/foo" + i, new byte[0], Ids.READ_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                j++;
            }
        } catch (ConnectionLossException e) {
                        waitForClientsConnected();
        }
        return j;
    }
    
    private void checkNodes(ZooKeeper zk, int start, int count) throws Exception {
        LOG.info("Validating nodes {} thru {}", start, (start + count));
        for (int i = start; i < start + count; i++) {
            Assert.assertNotNull(zk.exists("/foo" + i, false));
            LOG.error("Exists zxid:{}", Long.toHexString(zk.exists("/foo" + i, false).getCzxid()));
        }
        Assert.assertNull(zk.exists("/foo" + (start + count), false));
    }

    
    @Test
    public void testSimpleRolloverFollower() throws Exception {
        adjustEpochNearEnd();

        ZooKeeper zk = getClient((idxLeader == 1 ? 2 : 1));
        int countCreated = createNodes(zk, 0, 10);
        
        checkNodes(zk, 0, countCreated);
    }

    
    @Test
    public void testRolloverThenRestart() throws Exception {
        ZooKeeper zk = getClient(idxFollower);
        
        int countCreated = createNodes(zk, 0, 10);

        adjustEpochNearEnd();
        
        countCreated += createNodes(zk, countCreated, 10);

        shutdownAll();
        startAll();
        zk = getClient(idxLeader);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();
        
        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdownAll();
        startAll();
        zk = getClient(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdownAll();
        startAll();
        zk = getClient(idxLeader);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

                Assert.assertTrue(countCreated > 0);
        Assert.assertTrue(countCreated < 60);
    }

    
    @Test
    public void testRolloverThenFollowerRestart() throws Exception {
        ZooKeeper zk = getClient(idxFollower);

        int countCreated = createNodes(zk, 0, 10);

        adjustEpochNearEnd();
        
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxFollower);
        start(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();
        
        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxFollower);
        start(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxFollower);
        start(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

                Assert.assertTrue(countCreated > 0);
        Assert.assertTrue(countCreated < 60);
    }

    
    @Test
    public void testRolloverThenLeaderRestart() throws Exception {
        ZooKeeper zk = getClient(idxLeader);

        int countCreated = createNodes(zk, 0, 10);

        adjustEpochNearEnd();
        
        checkNodes(zk, 0, countCreated);

        shutdown(idxLeader);
        start(idxLeader);
        zk = getClient(idxLeader);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();
        
        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxLeader);
        start(idxLeader);
        zk = getClient(idxLeader);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxLeader);
        start(idxLeader);
        zk = getClient(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

                Assert.assertTrue(countCreated > 0);
        Assert.assertTrue(countCreated < 50);
    }

    
    @Test
    public void testMultipleRollover() throws Exception {
        ZooKeeper zk = getClient(idxFollower);

        int countCreated = createNodes(zk, 0, 10);

        adjustEpochNearEnd();
        
        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();

        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();
        
        countCreated += createNodes(zk, countCreated, 10);

        adjustEpochNearEnd();

        countCreated += createNodes(zk, countCreated, 10);

        shutdownAll();
        startAll();
        zk = getClient(idxFollower);

        adjustEpochNearEnd();

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

        shutdown(idxLeader);
        start(idxLeader);
        zk = getClient(idxFollower);

        checkNodes(zk, 0, countCreated);
        countCreated += createNodes(zk, countCreated, 10);

                Assert.assertTrue(countCreated > 0);
        Assert.assertTrue(countCreated < 70);
    }
}
