package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DuplicateLocalSessionUpgradeTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory
            .getLogger(DuplicateLocalSessionUpgradeTest.class);

    private final QuorumBase qb = new QuorumBase();

    private static final int CONNECTION_TIMEOUT = ClientBase.CONNECTION_TIMEOUT;

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
    public void testLocalSessionUpgradeOnFollower() throws Exception {
        testLocalSessionUpgrade(false);
    }

    @Test
    public void testLocalSessionUpgradeOnLeader() throws Exception {
        testLocalSessionUpgrade(true);
    }

    private void testLocalSessionUpgrade(boolean testLeader) throws Exception {

        int leaderIdx = qb.getLeaderIndex();
        Assert.assertFalse("No leader in quorum?", leaderIdx == -1);
        int followerIdx = (leaderIdx + 1) % 5;
        int testPeerIdx = testLeader ? leaderIdx : followerIdx;
        String hostPorts[] = qb.hostPort.split(",");

        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = qb.createClient(watcher, hostPorts[testPeerIdx],
                CONNECTION_TIMEOUT);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        final String firstPath = "/first";
        final String secondPath = "/ephemeral";

                zk.create(firstPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

                                zk.create(secondPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        Stat firstStat = zk.exists(firstPath, null);
        Assert.assertNotNull(firstStat);

        Stat secondStat = zk.exists(secondPath, null);
        Assert.assertNotNull(secondStat);

        long zxidDiff = secondStat.getCzxid() - firstStat.getCzxid();

                                Assert.assertEquals(2L, zxidDiff);

    }
}
