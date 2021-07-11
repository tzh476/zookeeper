package org.apache.zookeeper.test;

import static java.net.InetAddress.getLoopbackAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.jmx.CommonNames;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconfigTest extends ZKTestCase implements DataCallback{
    private static final Logger LOG = LoggerFactory
            .getLogger(ReconfigTest.class);

    private QuorumUtil qu;
    private ZooKeeper[] zkArr;
    private ZooKeeperAdmin[] zkAdminArr;

    @Before
    public void setup() {
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU=");
        QuorumPeerConfig.setReconfigEnabled(true);
    }

    @After
    public void tearDown() throws Exception {
        closeAllHandles(zkArr, zkAdminArr);
        if (qu != null) {
            qu.tearDown();
        }
    }

    public static String reconfig(ZooKeeperAdmin zkAdmin, List<String> joiningServers,
                                  List<String> leavingServers, List<String> newMembers, long fromConfig)
            throws KeeperException, InterruptedException {
        byte[] config = null;
        for (int j = 0; j < 30; j++) {
            try {
                config = zkAdmin.reconfigure(joiningServers, leavingServers,
                        newMembers, fromConfig, new Stat());
                break;
            } catch (KeeperException.ConnectionLossException e) {
                if (j < 29) {
                    Thread.sleep(1000);
                } else {
                                                            Assert.fail("client could not connect to reestablished quorum: giving up after 30+ seconds.");
                }
            }
        }

        String configStr = new String(config);
        if (joiningServers != null) {
            for (String joiner : joiningServers)
                Assert.assertTrue(configStr.contains(joiner));
        }
        if (leavingServers != null) {
            for (String leaving : leavingServers)
                Assert.assertFalse(configStr.contains("server.".concat(leaving)));
        }

        return configStr;
    }

    public static String testServerHasConfig(ZooKeeper zk,
            List<String> joiningServers, List<String> leavingServers)
            throws KeeperException, InterruptedException {
        boolean testNodeExists = false;
        byte[] config = null;
        for (int j = 0; j < 30; j++) {
            try {
                if (!testNodeExists) {
                    createZNode(zk, "/dummy", "dummy");
                    testNodeExists = true;
                }
                                                zk.setData("/dummy", "dummy".getBytes(), -1);
                config = zk.getConfig(false, new Stat());
                break;
            } catch (KeeperException.ConnectionLossException e) {
                if (j < 29) {
                    Thread.sleep(1000);
                } else {
                                                            Assert.fail("client could not connect to reestablished quorum: giving up after 30+ seconds.");
                }
            }
        }

        String configStr = new String(config);
        if (joiningServers != null) {
            for (String joiner : joiningServers) {
               Assert.assertTrue(configStr.contains(joiner));
            }
        }
        if (leavingServers != null) {
            for (String leaving : leavingServers)
                Assert.assertFalse(configStr.contains("server.".concat(leaving)));
        }

        return configStr;
    }

    public static void testNormalOperation(ZooKeeper writer, ZooKeeper reader)
            throws KeeperException, InterruptedException {
        boolean testReaderNodeExists = false;
        boolean testWriterNodeExists = false;

        for (int j = 0; j < 30; j++) {
            try {
                if (!testWriterNodeExists) {
                    createZNode(writer, "/test", "test");
                    testWriterNodeExists = true;
                }

                if (!testReaderNodeExists) {
                    createZNode(reader, "/dummy", "dummy");
                    testReaderNodeExists = true;
                }

                String data = "test" + j;
                writer.setData("/test", data.getBytes(), -1);
                                                reader.setData("/dummy", "dummy".getBytes(), -1);
                byte[] res = reader.getData("/test", null, new Stat());
                Assert.assertEquals(data, new String(res));
                break;
            } catch (KeeperException.ConnectionLossException e) {
                if (j < 29) {
                    Thread.sleep(1000);
                } else {
                                                            Assert.fail("client could not connect to reestablished quorum: giving up after 30+ seconds.");
                }
            }
        }
    }

    private static void createZNode(ZooKeeper zk, String path, String data)
            throws KeeperException, InterruptedException {
        try{
            zk.create(path, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
        }
    }
    
    private int getLeaderId(QuorumUtil qu) {
        int leaderId = 1;
        while (qu.getPeer(leaderId).peer.leader == null)
            leaderId++;
        return leaderId;
    }

    public static ZooKeeper[] createHandles(QuorumUtil qu) throws IOException {
                        ZooKeeper[] zkArr = new ZooKeeper[qu.ALL + 1];
        zkArr[0] = null;         for (int i = 1; i <= qu.ALL; i++) {
                        zkArr[i] = new ZooKeeper("127.0.0.1:"
                    + qu.getPeer(i).peer.getClientPort(),
                    ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                        public void process(WatchedEvent event) {
                        }});
        }
        return zkArr;
    }

    public static ZooKeeperAdmin[] createAdminHandles(QuorumUtil qu) throws IOException {
                        ZooKeeperAdmin[] zkAdminArr = new ZooKeeperAdmin[qu.ALL + 1];
        zkAdminArr[0] = null;         for (int i = 1; i <= qu.ALL; i++) {
                        zkAdminArr[i] = new ZooKeeperAdmin("127.0.0.1:"
                    + qu.getPeer(i).peer.getClientPort(),
                    ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {
                }});
            zkAdminArr[i].addAuthInfo("digest", "super:test".getBytes());
        }

        return zkAdminArr;
    }

    public static void closeAllHandles(ZooKeeper[] zkArr, ZooKeeperAdmin[] zkAdminArr) throws InterruptedException {
        if (zkArr != null) {
            for (ZooKeeper zk : zkArr)
                if (zk != null)
                    zk.close();
        }
        if (zkAdminArr != null) {
            for (ZooKeeperAdmin zkAdmin : zkAdminArr)
                if (zkAdmin != null)
                    zkAdmin.close();
        }
    }

    @Test
    public void testRemoveAddOne() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        List<String> joiningServers = new ArrayList<String>();

        int leaderIndex = getLeaderId(qu);

                        int leavingIndex = (leaderIndex == 1) ? 2 : 1;

        for (int i = 0; i < 2; i++) {
                                                                                    ZooKeeper zk1 = (leavingIndex == leaderIndex) ? zkArr[leaderIndex]
                    : zkArr[(leaderIndex % qu.ALL) + 1];
            ZooKeeper zk2 = (leavingIndex == leaderIndex) ? zkArr[(leaderIndex % qu.ALL) + 1]
                    : zkArr[leaderIndex];
            ZooKeeperAdmin zkAdmin1 = (leavingIndex == leaderIndex) ? zkAdminArr[leaderIndex]
                    : zkAdminArr[(leaderIndex % qu.ALL) + 1];
            ZooKeeperAdmin zkAdmin2 = (leavingIndex == leaderIndex) ? zkAdminArr[(leaderIndex % qu.ALL) + 1]
                    : zkAdminArr[leaderIndex];

            leavingServers.add(Integer.toString(leavingIndex));

                        joiningServers.add("server."
                    + leavingIndex
                    + "=localhost:"
                    + qu.getPeer(leavingIndex).peer.getQuorumAddress()
                            .getPort()
                    + ":"
                    + qu.getPeer(leavingIndex).peer.getElectionAddress()
                            .getPort() + ":participant;localhost:"
                    + qu.getPeer(leavingIndex).peer.getClientPort());

            String configStr = reconfig(zkAdmin1, null, leavingServers, null, -1);
            testServerHasConfig(zk2, null, leavingServers);
            testNormalOperation(zk2, zk1);

            QuorumVerifier qv = qu.getPeer(1).peer.configFromString(configStr);
            long version = qv.getVersion();

                        try {
                reconfig(zkAdmin2, joiningServers, null, null, version + 1);
                Assert.fail("reconfig succeeded even though version condition was incorrect!");
            } catch (KeeperException.BadVersionException e) {

            }

            reconfig(zkAdmin2, joiningServers, null, null, version);

            testNormalOperation(zk1, zk2);
            testServerHasConfig(zk1, joiningServers, null);

                                    leavingIndex = leaderIndex = getLeaderId(qu);
            leavingServers.clear();
            joiningServers.clear();
        }
    }

    
    @Test
    public void testRemoveAddTwo() throws Exception {
        qu = new QuorumUtil(2);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        List<String> joiningServers = new ArrayList<String>();

        int leaderIndex = getLeaderId(qu);

                int leavingIndex1 = leaderIndex;
        int leavingIndex2 = (leaderIndex == 1) ? 2 : 1;

                int stayingIndex1 = 1, stayingIndex2 = 1, stayingIndex3 = 1;
        while (stayingIndex1 == leavingIndex1 || stayingIndex1 == leavingIndex2)
            stayingIndex1++;

        while (stayingIndex2 == leavingIndex1 || stayingIndex2 == leavingIndex2
                || stayingIndex2 == stayingIndex1)
            stayingIndex2++;

        while (stayingIndex3 == leavingIndex1 || stayingIndex3 == leavingIndex2
                || stayingIndex3 == stayingIndex1
                || stayingIndex3 == stayingIndex2)
            stayingIndex3++;

        leavingServers.add(Integer.toString(leavingIndex1));
        leavingServers.add(Integer.toString(leavingIndex2));

                joiningServers.add("server." + leavingIndex1 + "=localhost:"
                + qu.getPeer(leavingIndex1).peer.getQuorumAddress().getPort()
                + ":"
                + qu.getPeer(leavingIndex1).peer.getElectionAddress().getPort()
                + ":participant;localhost:"
                + qu.getPeer(leavingIndex1).peer.getClientPort());

                joiningServers.add("server." + leavingIndex2 + "=localhost:"
                + qu.getPeer(leavingIndex2).peer.getQuorumAddress().getPort()
                + ":"
                + qu.getPeer(leavingIndex2).peer.getElectionAddress().getPort()
                + ":observer;localhost:"
                + qu.getPeer(leavingIndex2).peer.getClientPort());

        qu.shutdown(leavingIndex1);
        qu.shutdown(leavingIndex2);

                reconfig(zkAdminArr[stayingIndex2], null, leavingServers, null, -1);
        
        qu.shutdown(stayingIndex2);

                        
        testServerHasConfig(zkArr[stayingIndex1], null, leavingServers);
        testServerHasConfig(zkArr[stayingIndex3], null, leavingServers);
        testNormalOperation(zkArr[stayingIndex1], zkArr[stayingIndex3]);
        
                                
                        Thread.sleep(10000);

        try {
            reconfig(zkAdminArr[stayingIndex1], joiningServers, null, null, -1);
            Assert.fail("reconfig completed successfully even though there is no quorum up in new config!");
        } catch (KeeperException.NewConfigNoQuorum e) {

        }
        
                qu.restart(stayingIndex2);

        reconfig(zkAdminArr[stayingIndex1], joiningServers, null, null, -1);
        testNormalOperation(zkArr[stayingIndex2], zkArr[stayingIndex3]);
        testServerHasConfig(zkArr[stayingIndex2], joiningServers, null);

                        
        qu.restart(leavingIndex2);
        Assert.assertTrue(qu.getPeer(leavingIndex2).peer.getPeerState() == ServerState.OBSERVING);
        testNormalOperation(zkArr[stayingIndex2], zkArr[leavingIndex2]);
        testServerHasConfig(zkArr[leavingIndex2], joiningServers, null);
    }

    @Test
    public void testBulkReconfig() throws Exception {
        qu = new QuorumUtil(3);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

                        ArrayList<String> newServers = new ArrayList<String>();
        for (int i = 1; i <= 5; i++) {
            String server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":"
                    + ((i == 4 || i == 5) ? "observer" : "participant")
                    + ";localhost:" + qu.getPeer(i).peer.getClientPort();
            newServers.add(server);
        }

        qu.shutdown(3);
        qu.shutdown(6);
        qu.shutdown(7);
        
        reconfig(zkAdminArr[1], null, null, newServers, -1);
        testNormalOperation(zkArr[1], zkArr[2]);
       
        testServerHasConfig(zkArr[1], newServers, null);
        testServerHasConfig(zkArr[2], newServers, null);
        testServerHasConfig(zkArr[4], newServers, null);
        testServerHasConfig(zkArr[5], newServers, null);
    
        qu.shutdown(5);
        qu.shutdown(4);
        
        testNormalOperation(zkArr[1], zkArr[2]);
    }

    @Test
    public void testRemoveOneAsynchronous() throws Exception {
        qu = new QuorumUtil(2); 
        qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
       
                leavingServers.add(getLeaderId(qu) == 5 ? "4": "5");
 
        LinkedList<Integer> results = new LinkedList<Integer>();
        
        zkAdminArr[1].reconfigure(null, leavingServers, null, -1, this, results);
        
        synchronized (results) {
            while (results.size() < 1) {
               results.wait();
            }
        }        
        Assert.assertEquals(0, (int) results.get(0));
        
        testNormalOperation(zkArr[1], zkArr[2]);       
        for (int i=1; i<=5; i++)
            testServerHasConfig(zkArr[i], null, leavingServers);
    }

    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx, byte[] data,
            Stat stat) {
        synchronized(ctx) {
            ((LinkedList<Integer>)ctx).add(rc);
            ctx.notifyAll();
        }
    }
    
    
    @Test
    public void testRoleChange() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

                        List<String> joiningServers = new ArrayList<String>();

        int leaderIndex = getLeaderId(qu);

                                        int changingIndex = (leaderIndex == 1) ? 2 : 1;

                        String newRole = "observer";

        for (int i = 0; i < 4; i++) {
                                                                                    ZooKeeper zk1 = (changingIndex == leaderIndex) ? zkArr[leaderIndex]
                    : zkArr[(leaderIndex % qu.ALL) + 1];
            ZooKeeperAdmin zkAdmin1 = (changingIndex == leaderIndex) ? zkAdminArr[leaderIndex]
                    : zkAdminArr[(leaderIndex % qu.ALL) + 1];

                        joiningServers.add("server."
                    + changingIndex
                    + "=localhost:"
                    + qu.getPeer(changingIndex).peer.getQuorumAddress()
                            .getPort()
                    + ":"
                    + qu.getPeer(changingIndex).peer.getElectionAddress()
                            .getPort() + ":" + newRole + ";localhost:"
                    + qu.getPeer(changingIndex).peer.getClientPort());

            reconfig(zkAdmin1, joiningServers, null, null, -1);
            testNormalOperation(zkArr[changingIndex], zk1);

            if (newRole.equals("observer")) {
                Assert.assertTrue(qu.getPeer(changingIndex).peer.observer != null
                        && qu.getPeer(changingIndex).peer.follower == null
                        && qu.getPeer(changingIndex).peer.leader == null);
                Assert.assertTrue(qu.getPeer(changingIndex).peer.getPeerState() == ServerState.OBSERVING);
            } else {
                Assert.assertTrue(qu.getPeer(changingIndex).peer.observer == null
                        && (qu.getPeer(changingIndex).peer.follower != null || qu
                                .getPeer(changingIndex).peer.leader != null));
                Assert.assertTrue(qu.getPeer(changingIndex).peer.getPeerState() == ServerState.FOLLOWING
                        || qu.getPeer(changingIndex).peer.getPeerState() == ServerState.LEADING);
            }

            joiningServers.clear();

            if (newRole.equals("observer")) {
                newRole = "participant";
            } else {
                                newRole = "observer";
                leaderIndex = getLeaderId(qu);
                changingIndex = leaderIndex;
            }
        }
    }

    @Test
    public void testPortChange() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> joiningServers = new ArrayList<String>();

        int leaderIndex = getLeaderId(qu);
        int followerIndex = leaderIndex == 1 ? 2 : 1;

        
        int quorumPort = qu.getPeer(followerIndex).peer.getQuorumAddress().getPort();
        int electionPort = qu.getPeer(followerIndex).peer.getElectionAddress().getPort(); 
        int oldClientPort = qu.getPeer(followerIndex).peer.getClientPort();
        int newClientPort = PortAssignment.unique();
        joiningServers.add("server." + followerIndex + "=localhost:" + quorumPort
                + ":" + electionPort + ":participant;localhost:" + newClientPort);

                        testNormalOperation(zkArr[followerIndex], zkArr[leaderIndex]);

        reconfig(zkAdminArr[followerIndex], joiningServers, null, null, -1);

        try {
          for (int i=0; i < 20; i++) {
            Thread.sleep(1000);
            zkArr[followerIndex].setData("/test", "teststr".getBytes(), -1);
          }
        } catch (KeeperException.ConnectionLossException e) {
            Assert.fail("Existing client disconnected when client port changed!");
        }

        zkArr[followerIndex].close();
        zkArr[followerIndex] = new ZooKeeper("127.0.0.1:"
                + oldClientPort,
                ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                    public void process(WatchedEvent event) {}});

        zkAdminArr[followerIndex].close();
        zkAdminArr[followerIndex] = new ZooKeeperAdmin("127.0.0.1:"
                + oldClientPort,
                ClientBase.CONNECTION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent event) {}});
        zkAdminArr[followerIndex].addAuthInfo("digest", "super:test".getBytes());

        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(1000);
                zkArr[followerIndex].setData("/test", "teststr".getBytes(), -1);
                Assert.fail("New client connected to old client port!");
            } catch (KeeperException.ConnectionLossException e) {
            }
        }

        zkArr[followerIndex].close();
        zkArr[followerIndex] = new ZooKeeper("127.0.0.1:"
                + newClientPort,
                ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                    public void process(WatchedEvent event) {}});

        zkAdminArr[followerIndex].close();
        zkAdminArr[followerIndex] = new ZooKeeperAdmin("127.0.0.1:"
                + newClientPort,
                ClientBase.CONNECTION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent event) {}});
        zkAdminArr[followerIndex].addAuthInfo("digest", "super:test".getBytes());

        testNormalOperation(zkArr[followerIndex], zkArr[leaderIndex]);
        testServerHasConfig(zkArr[followerIndex], joiningServers, null);
        Assert.assertEquals(newClientPort, qu.getPeer(followerIndex).peer.getClientPort());

        joiningServers.clear();

        
        int newQuorumPort = PortAssignment.unique();
        joiningServers.add("server." + leaderIndex + "=localhost:"
                + newQuorumPort
                + ":"
                + qu.getPeer(leaderIndex).peer.getElectionAddress().getPort()
                + ":participant;localhost:"
                + qu.getPeer(leaderIndex).peer.getClientPort());

        reconfig(zkAdminArr[leaderIndex], joiningServers, null, null, -1);

        testNormalOperation(zkArr[followerIndex], zkArr[leaderIndex]);

        Assert.assertTrue(qu.getPeer(leaderIndex).peer.getQuorumAddress()
                .getPort() == newQuorumPort);

        joiningServers.clear();

        
        for (int i = 1; i <= 3; i++) {
            joiningServers.add("server." + i + "=localhost:"
                    + qu.getPeer(i).peer.getQuorumAddress().getPort() + ":"
                    + PortAssignment.unique() + ":participant;localhost:"
                    + qu.getPeer(i).peer.getClientPort());
        }

        reconfig(zkAdminArr[1], joiningServers, null, null, -1);

        leaderIndex = getLeaderId(qu);
        int follower1 = leaderIndex == 1 ? 2 : 1;
        int follower2 = 1;
        while (follower2 == leaderIndex || follower2 == follower1)
            follower2++;

        
        qu.shutdown(getLeaderId(qu));

        testNormalOperation(zkArr[follower2], zkArr[follower1]);
        testServerHasConfig(zkArr[follower1], joiningServers, null);
        testServerHasConfig(zkArr[follower2], joiningServers, null);
    }

    @Test
    public void testPortChangeToBlockedPortFollower() throws Exception {
        testPortChangeToBlockedPort(false);
    }
    @Test
    public void testPortChangeToBlockedPortLeader() throws Exception {
        testPortChangeToBlockedPort(true);
    }

    private void testPortChangeToBlockedPort(boolean testLeader) throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> joiningServers = new ArrayList<String>();

        int leaderIndex = getLeaderId(qu);
        int followerIndex = leaderIndex == 1 ? 2 : 1;
        int serverIndex = testLeader ? leaderIndex : followerIndex;
        int reconfigIndex = testLeader ? followerIndex : leaderIndex;

                int quorumPort = qu.getPeer(serverIndex).peer.getQuorumAddress().getPort();
        int electionPort = qu.getPeer(serverIndex).peer.getElectionAddress().getPort();
        int oldClientPort = qu.getPeer(serverIndex).peer.getClientPort();
        int newClientPort = PortAssignment.unique();

        try(ServerSocket ss = new ServerSocket()) {
            ss.bind(new InetSocketAddress(getLoopbackAddress(), newClientPort));

            joiningServers.add("server." + serverIndex + "=localhost:" + quorumPort
                        + ":" + electionPort + ":participant;localhost:" + newClientPort);

                                    testNormalOperation(zkArr[followerIndex], zkArr[leaderIndex]);

                        reconfig(zkAdminArr[reconfigIndex], joiningServers, null, null, -1);
            Thread.sleep(1000);

                        zkArr[serverIndex].close();
            zkArr[serverIndex] = new ZooKeeper("127.0.0.1:"
                    + newClientPort,
                    ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                        public void process(WatchedEvent event) {}});

            zkAdminArr[serverIndex].close();
            zkAdminArr[serverIndex] = new ZooKeeperAdmin("127.0.0.1:"
                    + newClientPort,
                    ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {}});

            try {
                Thread.sleep(1000);
                zkArr[serverIndex].setData("/test", "teststr".getBytes(), -1);
                Assert.fail("New client connected to new client port!");
            } catch (KeeperException.ConnectionLossException e) {
                            }

            
            try (ServerSocket ss2 = new ServerSocket()) {
                ss2.bind(new InetSocketAddress(getLoopbackAddress(), oldClientPort));
            }

                        joiningServers.clear();
            joiningServers.add("server." + serverIndex + "=localhost:" + quorumPort
                    + ":" + electionPort + ":participant;localhost:" + oldClientPort);

            reconfig(zkAdminArr[reconfigIndex], joiningServers, null, null, -1);

            zkArr[serverIndex].close();
            zkArr[serverIndex] = new ZooKeeper("127.0.0.1:"
                    + oldClientPort,
                    ClientBase.CONNECTION_TIMEOUT, new Watcher() {
                        public void process(WatchedEvent event) {}});

            testNormalOperation(zkArr[followerIndex], zkArr[leaderIndex]);
            testServerHasConfig(zkArr[serverIndex], joiningServers, null);
            Assert.assertEquals(oldClientPort, qu.getPeer(serverIndex).peer.getClientPort());
        }
    }

    @Test
    public void testUnspecifiedClientAddress() throws Exception {
    	int[] ports = {
                PortAssignment.unique(),
                PortAssignment.unique(),
                PortAssignment.unique()
    	};

    	String server = "server.0=localhost:" + ports[0] + ":" + ports[1] + ";" + ports[2];
    	QuorumServer qs = new QuorumServer(0, server);
    	Assert.assertEquals(qs.clientAddr.getHostString(), "0.0.0.0");
    	Assert.assertEquals(qs.clientAddr.getPort(), ports[2]);
    }
    
    @Test
    public void testQuorumSystemChange() throws Exception {
        qu = new QuorumUtil(3);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

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

        reconfig(zkAdminArr[1], null, null, members, -1);

                testNormalOperation(zkArr[2], zkArr[3]);
        testNormalOperation(zkArr[4], zkArr[5]);

        for (int i = 1; i <= 5; i++) {
            if (!(qu.getPeer(i).peer.getQuorumVerifier() instanceof QuorumHierarchical))
                Assert.fail("peer " + i
                        + " doesn't think the quorum system is Hieararchical!");
        }

        qu.shutdown(1);
        qu.shutdown(2);
        qu.shutdown(3);
        qu.shutdown(7);
        qu.shutdown(6);

                testNormalOperation(zkArr[4], zkArr[5]);

        qu.restart(1);
        qu.restart(2);

        members.clear();
        for (int i = 1; i <= 3; i++) {
            members.add("server." + i + "=127.0.0.1:"
                    + qu.getPeer(i).peer.getQuorumAddress().getPort() + ":"
                    + qu.getPeer(i).peer.getElectionAddress().getPort() + ";"
                    + "127.0.0.1:" + qu.getPeer(i).peer.getClientPort());
        }

        reconfig(zkAdminArr[1], null, null, members, -1);

                testNormalOperation(zkArr[1], zkArr[2]);

        qu.shutdown(4);
        qu.shutdown(5);

                testNormalOperation(zkArr[1], zkArr[2]);

        for (int i = 1; i <= 2; i++) {
            if (!(qu.getPeer(i).peer.getQuorumVerifier() instanceof QuorumMaj))
                Assert.fail("peer "
                        + i
                        + " doesn't think the quorum system is a majority quorum system!");
        }
    }
    
    @Test
    public void testInitialConfigHasPositiveVersion() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        testNormalOperation(zkArr[1], zkArr[2]);
        for (int i=1; i<4; i++) {
            String configStr = testServerHasConfig(zkArr[i], null, null);
            QuorumVerifier qv = qu.getPeer(i).peer.configFromString(configStr);
            long version = qv.getVersion();
            Assert.assertTrue(version == 0x100000000L);
        }
    }

    
    @Test
    public void testJMXBeanAfterRemoveAddOne() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

        List<String> leavingServers = new ArrayList<String>();
        List<String> joiningServers = new ArrayList<String>();

                int leavingIndex = 1;
        int replica2 = 2;
        QuorumPeer peer2 = qu.getPeer(replica2).peer;
        QuorumServer leavingQS2 = peer2.getView().get(Long.valueOf(leavingIndex));        
        String remotePeerBean2 = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + replica2 + ",name1=replica."
                + leavingIndex;
        assertRemotePeerMXBeanAttributes(leavingQS2, remotePeerBean2);

                int replica3 = 3;
        QuorumPeer peer3 = qu.getPeer(replica3).peer;
        QuorumServer leavingQS3 = peer3.getView().get(Long.valueOf(leavingIndex));
        String remotePeerBean3 = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + replica3 + ",name1=replica."
                + leavingIndex;
        assertRemotePeerMXBeanAttributes(leavingQS3, remotePeerBean3);

        ZooKeeper zk = zkArr[leavingIndex];
        ZooKeeperAdmin zkAdmin = zkAdminArr[leavingIndex];

        leavingServers.add(Integer.toString(leavingIndex));

                joiningServers.add("server." + leavingIndex + "=127.0.0.1:"
                + qu.getPeer(leavingIndex).peer.getQuorumAddress().getPort()
                + ":"
                + qu.getPeer(leavingIndex).peer.getElectionAddress().getPort()
                + ":participant;127.0.0.1:"
                + qu.getPeer(leavingIndex).peer.getClientPort());

                reconfig(zkAdmin, null, leavingServers, null, -1);

                QuorumPeer removedPeer = qu.getPeer(leavingIndex).peer;
        String localPeerBean = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + leavingIndex
                + ",name1=replica." + leavingIndex;
        assertLocalPeerMXBeanAttributes(removedPeer, localPeerBean, false);

                JMXEnv.ensureNone(remotePeerBean2);
                JMXEnv.ensureNone(remotePeerBean3);

                reconfig(zkAdmin, joiningServers, null, null, -1);

                assertLocalPeerMXBeanAttributes(removedPeer, localPeerBean, true);

                leavingQS2 = peer2.getView().get(Long.valueOf(leavingIndex));
        assertRemotePeerMXBeanAttributes(leavingQS2, remotePeerBean2);

                leavingQS3 = peer3.getView().get(Long.valueOf(leavingIndex));
        assertRemotePeerMXBeanAttributes(leavingQS3, remotePeerBean3);
    }

    
    @Test
    public void testJMXBeanAfterRoleChange() throws Exception {
        qu = new QuorumUtil(1);         qu.disableJMXTest = true;
        qu.startAll();
        zkArr = createHandles(qu);
        zkAdminArr = createAdminHandles(qu);

                        List<String> joiningServers = new ArrayList<String>();

                int changingIndex = 1;
        int replica2 = 2;
        QuorumPeer peer2 = qu.getPeer(replica2).peer;
        QuorumServer changingQS2 = peer2.getView().get(Long.valueOf(changingIndex));
        String remotePeerBean2 = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + replica2 + ",name1=replica."
                + changingIndex;
        assertRemotePeerMXBeanAttributes(changingQS2, remotePeerBean2);

                int replica3 = 3;
        QuorumPeer peer3 = qu.getPeer(replica3).peer;
        QuorumServer changingQS3 = peer3.getView().get(Long.valueOf(changingIndex));
        String remotePeerBean3 = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + replica3 + ",name1=replica."
                + changingIndex;
        assertRemotePeerMXBeanAttributes(changingQS3, remotePeerBean3);

        String newRole = "observer";

        ZooKeeper zk = zkArr[changingIndex];
        ZooKeeperAdmin zkAdmin = zkAdminArr[changingIndex];

                joiningServers.add("server." + changingIndex + "=127.0.0.1:"
                + qu.getPeer(changingIndex).peer.getQuorumAddress().getPort()
                + ":"
                + qu.getPeer(changingIndex).peer.getElectionAddress().getPort()
                + ":" + newRole + ";127.0.0.1:"
                + qu.getPeer(changingIndex).peer.getClientPort());

        reconfig(zkAdmin, joiningServers, null, null, -1);
        testNormalOperation(zkArr[changingIndex], zk);

        Assert.assertTrue(qu.getPeer(changingIndex).peer.observer != null
                && qu.getPeer(changingIndex).peer.follower == null
                && qu.getPeer(changingIndex).peer.leader == null);
        Assert.assertTrue(qu.getPeer(changingIndex).peer.getPeerState() == ServerState.OBSERVING);

        QuorumPeer qp = qu.getPeer(changingIndex).peer;
        String localPeerBeanName = CommonNames.DOMAIN
                + ":name0=ReplicatedServer_id" + changingIndex
                + ",name1=replica." + changingIndex;

                assertLocalPeerMXBeanAttributes(qp, localPeerBeanName, true);

                changingQS2 = peer2.getView().get(Long.valueOf(changingIndex));
        assertRemotePeerMXBeanAttributes(changingQS2, remotePeerBean2);

                changingQS3 = peer3.getView().get(Long.valueOf(changingIndex));
        assertRemotePeerMXBeanAttributes(changingQS3, remotePeerBean3);
    }

    private void assertLocalPeerMXBeanAttributes(QuorumPeer qp,
            String beanName, Boolean isPartOfEnsemble) throws Exception {
        Assert.assertEquals("Mismatches LearnerType!", qp.getLearnerType()
                .name(), JMXEnv.ensureBeanAttribute(beanName, "LearnerType"));
        Assert.assertEquals("Mismatches ClientAddress!",
                qp.getClientAddress().getHostString() + ":" + qp.getClientAddress().getPort(),
                JMXEnv.ensureBeanAttribute(beanName, "ClientAddress"));
        Assert.assertEquals("Mismatches LearnerType!",
                qp.getElectionAddress().getHostString() + ":" + qp.getElectionAddress().getPort(),
                JMXEnv.ensureBeanAttribute(beanName, "ElectionAddress"));
        Assert.assertEquals("Mismatches PartOfEnsemble!", isPartOfEnsemble,
                JMXEnv.ensureBeanAttribute(beanName, "PartOfEnsemble"));
        Assert.assertEquals("Mismatches ConfigVersion!", qp.getQuorumVerifier()
                .getVersion(), JMXEnv.ensureBeanAttribute(beanName,
                "ConfigVersion"));
        Assert.assertEquals("Mismatches QuorumSystemInfo!", qp
                .getQuorumVerifier().toString(), JMXEnv.ensureBeanAttribute(
                beanName, "QuorumSystemInfo"));
    }

    String getAddrPortFromBean(String beanName, String attribute) throws Exception {
        String name = (String) JMXEnv.ensureBeanAttribute(
                beanName, attribute);

        if ( ! name.contains(":") ) {
            return name;
        }

        return getNumericalAddrPort(name);
    }

    String getNumericalAddrPort(String name) throws UnknownHostException {
        String port = name.split(":")[1];
        String addr = name.split(":")[0];
        addr = InetAddress.getByName(addr).getHostAddress();
        return addr + ":" + port;
    }

    private void assertRemotePeerMXBeanAttributes(QuorumServer qs,
            String beanName) throws Exception {
        Assert.assertEquals("Mismatches LearnerType!", qs.type.name(),
                JMXEnv.ensureBeanAttribute(beanName, "LearnerType"));
        Assert.assertEquals("Mismatches ClientAddress!",
                getNumericalAddrPort(qs.clientAddr.getHostString() + ":" + qs.clientAddr.getPort()),
                getAddrPortFromBean(beanName, "ClientAddress") );
        Assert.assertEquals("Mismatches ElectionAddress!",
                getNumericalAddrPort(qs.electionAddr.getHostString() + ":" + qs.electionAddr.getPort()),
                getAddrPortFromBean(beanName, "ElectionAddress") );
        Assert.assertEquals("Mismatches QuorumAddress!",
                getNumericalAddrPort(qs.addr.getHostString() + ":" + qs.addr.getPort()),
                getAddrPortFromBean(beanName, "QuorumAddress") );
    }
}
