package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SessionUpgradeTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(SessionUpgradeTest.class);
    public static final int CONNECTION_TIMEOUT = ClientBase.CONNECTION_TIMEOUT;

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
    public void testLocalSessionsWithoutEphemeralOnFollower() throws Exception {
        testLocalSessionsWithoutEphemeral(false);
    }

    @Test
    public void testLocalSessionsWithoutEphemeralOnLeader() throws Exception {
        testLocalSessionsWithoutEphemeral(true);
    }

    private void testLocalSessionsWithoutEphemeral(boolean testLeader)
            throws Exception {
        String nodePrefix = "/testLocalSessions-"
            + (testLeader ? "leaderTest-" : "followerTest-");
        int leaderIdx = qb.getLeaderIndex();
        Assert.assertFalse("No leader in quorum?", leaderIdx == -1);
        int followerIdx = (leaderIdx + 1) % 5;
        int otherFollowerIdx = (leaderIdx + 2) % 5;
        int testPeerIdx = testLeader ? leaderIdx : followerIdx;
        String hostPorts[] = qb.hostPort.split(",");
        CountdownWatcher watcher = new CountdownWatcher();
        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(
                hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

                for (int i = 0; i < 5; i++) {
            zk.create(nodePrefix + i, new byte[0],
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        long localSessionId = zk.getSessionId();
        byte[] localSessionPwd = zk.getSessionPasswd().clone();

                        try {
            watcher.reset();
            DisconnectableZooKeeper zknew = new DisconnectableZooKeeper(
                    hostPorts[otherFollowerIdx], CONNECTION_TIMEOUT, watcher,
                    localSessionId, localSessionPwd);

            zknew.create(nodePrefix + "5", new byte[0],
                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Assert.fail("Connection on the same session ID should fail.");
        } catch (KeeperException.SessionExpiredException e) {
        } catch (KeeperException.ConnectionLossException e) {
        }

                        if (!testLeader) {
            try {
                watcher.reset();
                DisconnectableZooKeeper zknew = new DisconnectableZooKeeper(
                        hostPorts[leaderIdx], CONNECTION_TIMEOUT,
                        watcher, localSessionId, localSessionPwd);

                zknew.create(nodePrefix + "5", new byte[0],
                             ZooDefs.Ids.OPEN_ACL_UNSAFE,
                             CreateMode.PERSISTENT);
                Assert.fail("Connection on the same session ID should fail.");
            } catch (KeeperException.SessionExpiredException e) {
            } catch (KeeperException.ConnectionLossException e) {
            }
        }

                                zk.disconnect();

        watcher.reset();
        zk = new DisconnectableZooKeeper(
                hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher,
                localSessionId, localSessionPwd);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        zk.create(nodePrefix + "6", new byte[0],
                  ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                        zk.close();
        try {
            watcher.reset();
            zk = new DisconnectableZooKeeper(
                    hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher,
                    localSessionId, localSessionPwd);

            zk.create(nodePrefix + "7", new byte[0],
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Assert.fail("Reconnecting to a closed session ID should fail.");
        } catch (KeeperException.SessionExpiredException e) {
        }
    }

    @Test
    public void testUpgradeWithEphemeralOnFollower() throws Exception {
        testUpgradeWithEphemeral(false);
    }

    @Test
    public void testUpgradeWithEphemeralOnLeader() throws Exception {
        testUpgradeWithEphemeral(true);
    }

    private void testUpgradeWithEphemeral(boolean testLeader)
            throws Exception {
        String nodePrefix = "/testUpgrade-"
            + (testLeader ? "leaderTest-" : "followerTest-");
        int leaderIdx = qb.getLeaderIndex();
        Assert.assertFalse("No leader in quorum?", leaderIdx == -1);
        int followerIdx = (leaderIdx + 1) % 5;
        int otherFollowerIdx = (leaderIdx + 2) % 5;
        int testPeerIdx = testLeader ? leaderIdx : followerIdx;
        String hostPorts[] = qb.hostPort.split(",");

        CountdownWatcher watcher = new CountdownWatcher();
        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(
                hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

                        for (int i = 0; i < 5; i++) {
            zk.create(nodePrefix + i, new byte[0],
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

                        long localSessionId = zk.getSessionId();
        byte[] localSessionPwd = zk.getSessionPasswd().clone();

        zk.disconnect();
        watcher.reset();
        zk = new DisconnectableZooKeeper(
                hostPorts[otherFollowerIdx], CONNECTION_TIMEOUT, watcher,
                localSessionId, localSessionPwd);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

                for (int i = 0; i < 5; i++) {
            Assert.assertNotNull(zk.exists(nodePrefix + i, null));
        }

                        zk.close();

        try {
            watcher.reset();
            zk = new DisconnectableZooKeeper(
                    hostPorts[otherFollowerIdx], CONNECTION_TIMEOUT, watcher,
                    localSessionId, localSessionPwd);
            zk.exists(nodePrefix + "0", null);
            Assert.fail("Reconnecting to a closed session ID should fail.");
        } catch (KeeperException.SessionExpiredException e) {
        }

        watcher.reset();
                zk = new DisconnectableZooKeeper(
                hostPorts[testPeerIdx], CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);
        for (int i = 0; i < 5; i++) {
            Assert.assertNull(zk.exists(nodePrefix + i, null));
        }
    }
}
