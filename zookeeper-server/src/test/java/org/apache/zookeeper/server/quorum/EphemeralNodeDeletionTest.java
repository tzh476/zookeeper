package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.security.sasl.SaslException;

public class EphemeralNodeDeletionTest extends QuorumPeerTestBase {
    private static int SERVER_COUNT = 3;
    private MainThread[] mt = new MainThread[SERVER_COUNT];

    

    @Test(timeout = 120000)
    public void testEphemeralNodeDeletion() throws Exception {
        final int clientPorts[] = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        String server;

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=127.0.0.1:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;127.0.0.1:"
                    + clientPorts[i];
            sb.append(server + "\n");
        }
        String currentQuorumCfgSection = sb.toString();
                for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    false) {
                @Override
                public TestQPMain getTestQPMain() {
                    return new MockTestQPMain();
                }
            };
            mt[i].start();
        }

                for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT));
        }

        CountdownWatcher watch = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper("127.0.0.1:" + clientPorts[1],
                ClientBase.CONNECTION_TIMEOUT, watch);
        watch.waitForConnected(ClientBase.CONNECTION_TIMEOUT);

        

        Stat firstEphemeralNode = new Stat();

                String nodePath = "/e1";
        zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, firstEphemeralNode);
        assertEquals("Current session and ephemeral owner should be same",
                zk.getSessionId(), firstEphemeralNode.getEphemeralOwner());

                CustomQuorumPeer follower = (CustomQuorumPeer) getByServerState(mt,
                ServerState.FOLLOWING);
        follower.setInjectError(true);

                zk.close();

                follower.setInjectError(false);

        Assert.assertTrue("Faulted Follower should have joined quorum by now",
                ClientBase.waitForServerUp(
                        "127.0.0.1:" + follower.getClientPort(),
                        CONNECTION_TIMEOUT));

        QuorumPeer leader = getByServerState(mt, ServerState.LEADING);
        assertNotNull("Leader should not be null", leader);
        Assert.assertTrue("Leader must be running", ClientBase.waitForServerUp(
                "127.0.0.1:" + leader.getClientPort(), CONNECTION_TIMEOUT));

        watch = new CountdownWatcher();
        zk = new ZooKeeper("127.0.0.1:" + leader.getClientPort(),
                ClientBase.CONNECTION_TIMEOUT, watch);
        watch.waitForConnected(ClientBase.CONNECTION_TIMEOUT);

        Stat exists = zk.exists(nodePath, false);
        assertNull("Node must have been deleted from leader", exists);

        CountdownWatcher followerWatch = new CountdownWatcher();
        ZooKeeper followerZK = new ZooKeeper(
                "127.0.0.1:" + follower.getClientPort(),
                ClientBase.CONNECTION_TIMEOUT, followerWatch);
        followerWatch.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        Stat nodeAtFollower = followerZK.exists(nodePath, false);

                assertNull("ephemeral node must not exist", nodeAtFollower);

                Stat currentEphemeralNode = new Stat();
        zk.create(nodePath, "2".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, currentEphemeralNode);

                zk.close();
        
        SyncCallback cb = new SyncCallback();
        followerZK.sync(nodePath, cb, null);
        cb.sync.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        
        nodeAtFollower = followerZK.exists(nodePath, false);

                                assertNull("After session close ephemeral node must be deleted",
                nodeAtFollower);
        followerZK.close();
    }

    @After
    public void tearDown() {
                for (int i = 0; i < mt.length; i++) {
            try {
                mt[i].shutdown();
            } catch (InterruptedException e) {
                LOG.warn("Quorum Peer interrupted while shutting it down", e);
            }
        }
    }

    private QuorumPeer getByServerState(MainThread[] mt, ServerState state) {
        for (int i = mt.length - 1; i >= 0; i--) {
            QuorumPeer quorumPeer = mt[i].getQuorumPeer();
            if (null != quorumPeer && state == quorumPeer.getPeerState()) {
                return quorumPeer;
            }
        }
        return null;
    }

    static class CustomQuorumPeer extends QuorumPeer {
        private boolean injectError = false;

        public CustomQuorumPeer() throws SaslException {

        }

        @Override
        protected Follower makeFollower(FileTxnSnapLog logFactory)
                throws IOException {
            return new Follower(this, new FollowerZooKeeperServer(logFactory,
                    this, this.getZkDb())) {

                @Override
                void readPacket(QuorumPacket pp) throws IOException {
                    
                    super.readPacket(pp);
                    if (injectError && pp.getType() == Leader.PROPOSAL) {
                        String type = LearnerHandler.packetToString(pp);
                        throw new SocketTimeoutException(
                                "Socket timeout while reading the packet for operation "
                                        + type);
                    }
                }

            };
        }

        public void setInjectError(boolean injectError) {
            this.injectError = injectError;
        }

    }

    static class MockTestQPMain extends TestQPMain {
        @Override
        protected QuorumPeer getQuorumPeer() throws SaslException {
            return new CustomQuorumPeer();
        }
    }
    
    private static class SyncCallback implements AsyncCallback.VoidCallback {
        private final CountDownLatch sync = new CountDownLatch(1);
        
        @Override
        public void processResult(int rc, String path, Object ctx) {
        	sync.countDown();
        }
    }
}
