package org.apache.zookeeper.test;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.TraceFormatter;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalSessionRequestTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory
            .getLogger(LocalSessionRequestTest.class);
        public static final int CONNECTION_TIMEOUT = 4000;

    private final QuorumBase qb = new QuorumBase();

    @Before
    public void setUp() throws Exception {
        LOG.info("STARTING quorum " + getClass().getName());
        qb.localSessionsEnabled = true;
        qb.localSessionsUpgradingEnabled = true;
        qb.setUp();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("STOPPING quorum " + getClass().getName());
        qb.tearDown();
    }

    @Test
    public void testLocalSessionsOnFollower() throws Exception {
        testOpenCloseSession(false);
    }

    @Test
    public void testLocalSessionsOnLeader() throws Exception {
        testOpenCloseSession(true);
    }

    
    private void validateRequestLog(long sessionId, int peerId) {
        String session = Long.toHexString(sessionId);
        LOG.info("Searching for txn of session 0x " + session +
                " on peer " + peerId);
        String peerType = peerId == qb.getLeaderIndex() ? "leader" : "follower";
        QuorumPeer peer = qb.getPeerList().get(peerId);
        ZKDatabase db = peer.getActiveServer().getZKDatabase();
        for (Proposal p : db.getCommittedLog()) {
            Assert.assertFalse("Should not see " +
                               TraceFormatter.op2String(p.request.type) +
                               " request from local session 0x" + session +
                               " on the " + peerType,
                               p.request.sessionId == sessionId);
        }
    }

    
    public void testOpenCloseSession(boolean onLeader) throws Exception {
        int leaderIdx = qb.getLeaderIndex();
        Assert.assertFalse("No leader in quorum?", leaderIdx == -1);
        int followerIdx = (leaderIdx + 1) % 5;
        int testPeerIdx = onLeader ? leaderIdx : followerIdx;
        int verifyPeerIdx = onLeader ? followerIdx : leaderIdx;

        String hostPorts[] = qb.hostPort.split(",");

        CountdownWatcher watcher = new CountdownWatcher();
        DisconnectableZooKeeper client = new DisconnectableZooKeeper(
                hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        long localSessionId1 = client.getSessionId();

                        client.dontReconnect();
        client.disconnect();
        watcher.reset();

        
        ZooKeeper zk = qb.createClient(watcher, hostPorts[testPeerIdx],
                CONNECTION_TIMEOUT);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        long localSessionId2 = zk.getSessionId();

                zk.close();
        watcher.reset();

                                        Thread.sleep(CONNECTION_TIMEOUT * 2);

                validateRequestLog(localSessionId1, verifyPeerIdx);

                validateRequestLog(localSessionId2, verifyPeerIdx);

        qb.shutdownServers();

    }
}
