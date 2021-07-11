package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NewConfigNoQuorum;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.QuorumUtil;
import org.apache.zookeeper.test.ReconfigTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReconfigFailureCasesTest extends QuorumPeerTestBase {

    private QuorumUtil qu;

    @Before
    public void setup() {
        QuorumPeerConfig.setReconfigEnabled(true);
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU=");
    }

    @After
    public void tearDown() throws Exception {
        if (qu != null) {
            qu.tearDown();
        }
    }

    
    @Test
    public void testIncrementalReconfigInvokedOnHiearchicalQS() throws Exception {
        qu = new QuorumUtil(2);         qu.disableJMXTest = true;
        qu.startAll();
        ZooKeeper[] zkArr = ReconfigTest.createHandles(qu);
        ZooKeeperAdmin[] zkAdminArr = ReconfigTest.createAdminHandles(qu);

        ArrayList<String> members = new ArrayList<String>();
        members.add("group.1=3:4:5");
        members.add("group.2=1:2");
        members.add("weight.1=0");
        members.add("weight.2=0");
        members.add("weight.3=1");
        members.add("weight.4=1");
        members.add("weight.5=1");

        for (int i = 1; i <= 5; i++) {
            members.add("server." + i + "=127.0.0.1:"
                    + qu.getPeer(i).peer.getQuorumAddress().getPort() + ":"
                    + qu.getPeer(i).peer.getElectionAddress().getPort() + ";"
                    + "127.0.0.1:" + qu.getPeer(i).peer.getClientPort());
        }

                ReconfigTest.reconfig(zkAdminArr[1], null, null, members, -1);
        ReconfigTest.testNormalOperation(zkArr[1], zkArr[2]);

                List<String> leavingServers = new ArrayList<String>();
        leavingServers.add("3");
        try {
             zkAdminArr[1].reconfigure(null, leavingServers, null, -1, null);
            Assert.fail("Reconfig should have failed since the current config isn't Majority QS");
        } catch (KeeperException.BadArgumentsException e) {
                    } catch (Exception e) {
            Assert.fail("Should have been BadArgumentsException!");
        }

        ReconfigTest.closeAllHandles(zkArr, zkAdminArr);
    }

    
    @Test
    public void testTooFewRemainingPariticipants() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        ZooKeeper[] zkArr = ReconfigTest.createHandles(qu);
        ZooKeeperAdmin[] zkAdminArr = ReconfigTest.createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        leavingServers.add("2");
        leavingServers.add("3");
        try {
             zkAdminArr[1].reconfigure(null, leavingServers, null, -1, null);
            Assert.fail("Reconfig should have failed since the current config version is not 8");
        } catch (KeeperException.BadArgumentsException e) {
                    } catch (Exception e) {
            Assert.fail("Should have been BadArgumentsException!");
        }

        ReconfigTest.closeAllHandles(zkArr, zkAdminArr);
    }

    
    @Test
    public void testReconfigVersionConditionFails() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        ZooKeeper[] zkArr = ReconfigTest.createHandles(qu);
        ZooKeeperAdmin[] zkAdminArr = ReconfigTest.createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        leavingServers.add("3");
        try {
             zkAdminArr[1].reconfigure(null, leavingServers, null, 8, null);
            Assert.fail("Reconfig should have failed since the current config version is not 8");
        } catch (KeeperException.BadVersionException e) {
                    } catch (Exception e) {
            Assert.fail("Should have been BadVersionException!");
        }

        ReconfigTest.closeAllHandles(zkArr, zkAdminArr);
    }

    
    @Test
    public void testLeaderTimesoutOnNewQuorum() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        ZooKeeper[] zkArr = ReconfigTest.createHandles(qu);
        ZooKeeperAdmin[] zkAdminArr = ReconfigTest.createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        leavingServers.add("3");
        qu.shutdown(2);
        try {
                                                                                                zkAdminArr[1].reconfigure(null, leavingServers, null, -1, null);
            Assert.fail("Reconfig should have failed since we don't have quorum of new config");
        } catch (KeeperException.ConnectionLossException e) {
                    } catch (Exception e) {
            Assert.fail("Should have been ConnectionLossException!");
        }

                                        Assert.assertEquals(QuorumStats.Provider.LOOKING_STATE,
                qu.getPeer(1).peer.getServerState());
        Assert.assertEquals(QuorumStats.Provider.LOOKING_STATE,
                qu.getPeer(3).peer.getServerState());
        ReconfigTest.closeAllHandles(zkArr, zkAdminArr);
    }

    
    @Test
    public void testObserverToParticipantConversionFails() throws Exception {
        ClientBase.setupTestEnv();

        final int SERVER_COUNT = 4;
        int[][] ports = ReconfigRecoveryTest.generatePorts(SERVER_COUNT);

                HashSet<Integer> observers = new HashSet<Integer>();
        observers.add(3);
        StringBuilder sb = ReconfigRecoveryTest.generateConfig(SERVER_COUNT, ports, observers);
        String currentQuorumCfgSection = sb.toString();
        String nextQuorumCfgSection = currentQuorumCfgSection.replace("observer", "participant");

        MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];
        ZooKeeperAdmin zkAdmin[] = new ZooKeeperAdmin[SERVER_COUNT];

                for (int i = 1; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, ports[i][2], currentQuorumCfgSection,
                    true, "100000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + ports[i][2],
                    ClientBase.CONNECTION_TIMEOUT, this);
            zkAdmin[i] = new ZooKeeperAdmin("127.0.0.1:" + ports[i][2],
                    ClientBase.CONNECTION_TIMEOUT, this);
            zkAdmin[i].addAuthInfo("digest", "super:test".getBytes());
        }

        for (int i = 1; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + ports[i][2],
                            CONNECTION_TIMEOUT * 2));
        }

        try {
            zkAdmin[1].reconfigure("", "", nextQuorumCfgSection, -1, new Stat());
            Assert.fail("Reconfig should have failed with NewConfigNoQuorum");
        } catch (NewConfigNoQuorum e) {
                                } catch (Exception e) {
            Assert.fail("Reconfig should have failed with NewConfigNoQuorum");
        }
                ArrayList<String> leavingServers = new ArrayList<String>();
        leavingServers.add("3");
        ReconfigTest.reconfig(zkAdmin[1], null, leavingServers, null, -1);
        ReconfigTest.testNormalOperation(zk[2], zk[3]);
        ReconfigTest.testServerHasConfig(zk[3], null, leavingServers);

                List<String> newMembers = Arrays.asList(nextQuorumCfgSection.split("\n"));
        ReconfigTest.reconfig(zkAdmin[1], null, null, newMembers, -1);
        ReconfigTest.testNormalOperation(zk[2], zk[3]);
        for (int i = 1; i < SERVER_COUNT; i++) {
            ReconfigTest.testServerHasConfig(zk[i], newMembers, null);
        }
        for (int i = 1; i < SERVER_COUNT; i++) {
            zk[i].close();
            zkAdmin[i].close();
            mt[i].shutdown();
        }
    }
}
