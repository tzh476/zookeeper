package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ReconfigTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReconfigRecoveryTest extends QuorumPeerTestBase {
    @Before
    public void setup() {
        QuorumPeerConfig.setReconfigEnabled(true);
    }

    
    @Test
    public void testNextConfigCompletion() throws Exception {
        ClientBase.setupTestEnv();

                final int SERVER_COUNT = 3;
        final int clientPorts[] = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        String server;
        ArrayList<String> allServers = new ArrayList<String>();

        String currentQuorumCfgSection = null, nextQuorumCfgSection;

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + clientPorts[i];
            allServers.add(server);
            sb.append(server + "\n");
            if (i == 1)
                currentQuorumCfgSection = sb.toString();
        }
        nextQuorumCfgSection = sb.toString();

                                MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];
        for (int i = 0; i < SERVER_COUNT - 1; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    true, "100000000");
                                                            mt[i].writeTempDynamicConfigFile(nextQuorumCfgSection, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

        Assert.assertTrue("waiting for server 0 being up", ClientBase
                .waitForServerUp("127.0.0.1:" + clientPorts[0],
                        CONNECTION_TIMEOUT));
        Assert.assertTrue("waiting for server 1 being up", ClientBase
                .waitForServerUp("127.0.0.1:" + clientPorts[1],
                        CONNECTION_TIMEOUT));

        int leader = mt[0].main.quorumPeer.leader == null ? 1 : 0;

                sb = new StringBuilder();
        sb.append(allServers.get(leader) + "\n");
        sb.append(allServers.get(2) + "\n");

                String newServerInitialConfig = sb.toString();
        mt[2] = new MainThread(2, clientPorts[2], newServerInitialConfig);
        mt[2].start();
        zk[2] = new ZooKeeper("127.0.0.1:" + clientPorts[2],
                ClientBase.CONNECTION_TIMEOUT, this);

        Assert.assertTrue("waiting for server 2 being up", ClientBase
                .waitForServerUp("127.0.0.1:" + clientPorts[2],
                        CONNECTION_TIMEOUT));

        ReconfigTest.testServerHasConfig(zk[0], allServers, null);
        ReconfigTest.testServerHasConfig(zk[1], allServers, null);
        ReconfigTest.testServerHasConfig(zk[2], allServers, null);

        ReconfigTest.testNormalOperation(zk[0], zk[2]);
        ReconfigTest.testNormalOperation(zk[2], zk[1]);

        for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i].shutdown();
            zk[i].close();
        }
    }

    
    @Test
    public void testCurrentServersAreObserversInNextConfig() throws Exception {
        ClientBase.setupTestEnv();

                final int SERVER_COUNT = 5;
        final int clientPorts[] = new int[SERVER_COUNT];
        final int oldClientPorts[] = new int[2];
        StringBuilder sb = new StringBuilder();
        String server;

        String currentQuorumCfg, nextQuorumCfgSection;

        ArrayList<String> allServersNext = new ArrayList<String>();

        for (int i = 0; i < 2; i++) {
            oldClientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + oldClientPorts[i];
            sb.append(server + "\n");
        }

        currentQuorumCfg = sb.toString();

        sb = new StringBuilder();
        String role;
        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            if (i < 2) {
                role = "observer";
            } else {
                role = "participant";
            }
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":" + role
                    + ";localhost:" + clientPorts[i];
            allServersNext.add(server);
            sb.append(server + "\n");
        }
        nextQuorumCfgSection = sb.toString();

        MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];

                for (int i = 0; i < 2; i++) {
            mt[i] = new MainThread(i, oldClientPorts[i], currentQuorumCfg,
                    true, "100000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + oldClientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

        for (int i = 0; i < 2; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp(
                            "127.0.0.1:" + oldClientPorts[i],
                            CONNECTION_TIMEOUT * 2));
        }

        ReconfigTest.testNormalOperation(zk[0], zk[1]);

                for (int i = 0; i < 2; i++) {
            mt[i].shutdown();
            zk[i].close();
        }

        for (int i = 0; i < 2; i++) {
            Assert.assertTrue(
                    "waiting for server " + i + " being up",
                    ClientBase.waitForServerDown("127.0.0.1:"
                            + oldClientPorts[i], CONNECTION_TIMEOUT * 2));
        }

        for (int i = 0; i < 2; i++) {
            mt[i].writeTempDynamicConfigFile(nextQuorumCfgSection, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

                for (int i = 2; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfg
                    + allServersNext.get(i));
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

        for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT * 2));
            ReconfigTest.testServerHasConfig(zk[i], allServersNext, null);
        }

        ReconfigTest.testNormalOperation(zk[0], zk[2]);
        ReconfigTest.testNormalOperation(zk[4], zk[1]);

        for (int i = 0; i < SERVER_COUNT; i++) {
            zk[i].close();
            mt[i].shutdown();
        }
    }

    
    @Test
    public void testNextConfigUnreachable() throws Exception {
        ClientBase.setupTestEnv();

                final int SERVER_COUNT = 5;
        final int clientPorts[] = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        String server;

        String currentQuorumCfgSection = null, nextQuorumCfgSection;

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + clientPorts[i];
            sb.append(server + "\n");
            if (i == 1)
                currentQuorumCfgSection = sb.toString();
        }
        nextQuorumCfgSection = sb.toString();

        MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];

                        for (int i = 0; i < 2; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    true, "100000000");
                                                mt[i].writeTempDynamicConfigFile(nextQuorumCfgSection, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

        Thread.sleep(CONNECTION_TIMEOUT * 2);

                        for (int i = 0; i < 2; i++) {
            Assert.assertFalse("server " + i + " is up but shouldn't be",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT / 10));
        }

        for (int i = 0; i < 2; i++) {
            zk[i].close();
            mt[i].shutdown();
        }
    }

    
    @Test
    public void testNextConfigAlreadyActive() throws Exception {
        ClientBase.setupTestEnv();

                final int SERVER_COUNT = 5;
        final int clientPorts[] = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        String server;

        String currentQuorumCfgSection = null, nextQuorumCfgSection;

        ArrayList<String> allServers = new ArrayList<String>();
        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + clientPorts[i];
            allServers.add(server);
            sb.append(server + "\n");
            if (i == 1) currentQuorumCfgSection = sb.toString();
        }
        nextQuorumCfgSection = sb.toString();

                MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];
        for (int i = 2; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], nextQuorumCfgSection,
                    true, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }
        for (int i = 2; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT));
        }

        ReconfigTest.testNormalOperation(zk[2], zk[3]);

        long epoch = mt[2].main.quorumPeer.getAcceptedEpoch();

                                for (int i = 0; i < 2; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    true, "100000000");
            mt[i].writeTempDynamicConfigFile(nextQuorumCfgSection, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

                        for (int i = 0; i < 2; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT * 2));
        }

                Assert.assertEquals(epoch, mt[0].main.quorumPeer.getAcceptedEpoch());
        Assert.assertEquals(epoch, mt[1].main.quorumPeer.getAcceptedEpoch());
        Assert.assertEquals(epoch, mt[2].main.quorumPeer.getAcceptedEpoch());

        ReconfigTest.testServerHasConfig(zk[0], allServers, null);
        ReconfigTest.testServerHasConfig(zk[1], allServers, null);

        ReconfigTest.testNormalOperation(zk[0], zk[2]);
        ReconfigTest.testNormalOperation(zk[4], zk[1]);

        for (int i = 0; i < SERVER_COUNT; i++) {
            zk[i].close();
            mt[i].shutdown();
        }
    }

    
    @Test
    public void testObserverConvertedToParticipantDuringFLE() throws Exception {
        ClientBase.setupTestEnv();

        final int SERVER_COUNT = 4;
        int[][] ports = generatePorts(SERVER_COUNT);
        String currentQuorumCfgSection, nextQuorumCfgSection;

                HashSet<Integer> observers = new HashSet<Integer>();
        observers.add(2);
        StringBuilder sb = generateConfig(3, ports, observers);
        currentQuorumCfgSection = sb.toString();

                ArrayList<String> allServersNext = new ArrayList<String>();
        sb = new StringBuilder();
        for (int i = 2; i < SERVER_COUNT; i++) {
            String server = "server." + i + "=localhost:" + ports[i][0] + ":"
                    + ports[i][1] + ":participant;localhost:" + ports[i][2];
            allServersNext.add(server);
            sb.append(server + "\n");
        }
        nextQuorumCfgSection = sb.toString();

        MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];

                mt[2] = new MainThread(2, ports[2][2], currentQuorumCfgSection,
                true, "100000000");
        mt[2].start();
        zk[2] = new ZooKeeper("127.0.0.1:" + ports[2][2],
                ClientBase.CONNECTION_TIMEOUT, this);

                mt[3] = new MainThread(3, ports[3][2], nextQuorumCfgSection,
                true, "200000000");
        mt[3].start();
        zk[3] = new ZooKeeper("127.0.0.1:" + ports[3][2],
                ClientBase.CONNECTION_TIMEOUT, this);

        for (int i = 2; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + ports[i][2],
                            CONNECTION_TIMEOUT * 2));
            ReconfigTest.testServerHasConfig(zk[i], allServersNext, null);
        }

        Assert.assertEquals(nextQuorumCfgSection + "version=200000000",
                ReconfigTest.testServerHasConfig(zk[2], null, null));
        Assert.assertEquals(nextQuorumCfgSection + "version=200000000",
                ReconfigTest.testServerHasConfig(zk[3], null, null));
        ReconfigTest.testNormalOperation(zk[2], zk[2]);
        ReconfigTest.testNormalOperation(zk[3], zk[2]);

        for (int i = 2; i < SERVER_COUNT; i++) {
            zk[i].close();
            mt[i].shutdown();
        }
    }

    
    @Test
    public void testCurrentObserverIsParticipantInNewConfig() throws Exception {
        ClientBase.setupTestEnv();

        final int SERVER_COUNT = 4;
        int[][] ports = generatePorts(SERVER_COUNT);
        String currentQuorumCfg, nextQuorumCfgSection;

                HashSet<Integer> observers = new HashSet<Integer>();
        observers.add(2);

        StringBuilder sb = generateConfig(3, ports, observers);
        currentQuorumCfg = sb.toString();

                MainThread mt[] = new MainThread[SERVER_COUNT];
        ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];
        for (int i = 0; i <= 2; i++) {
            mt[i] = new MainThread(i, ports[i][2], currentQuorumCfg
                    , true, "100000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + ports[i][2],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }

        ReconfigTest.testNormalOperation(zk[0], zk[2]);

        for (int i = 0; i <= 2; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + ports[i][2],
                            CONNECTION_TIMEOUT * 2));
        }

                for (int i = 0; i <= 2; i++) {
            mt[i].shutdown();
            zk[i].close();
        }

                ArrayList<String> allServersNext = new ArrayList<String>();
        sb = new StringBuilder();
        for (int i = 2; i < SERVER_COUNT; i++) {
            String server = "server." + i + "=localhost:" + ports[i][0] + ":"
                    + ports[i][1] + ":participant;localhost:" + ports[i][2];
            allServersNext.add(server);
            sb.append(server + "\n");
        }
        nextQuorumCfgSection = sb.toString();

                        for (int i = 0; i <= 2; i++) {
            mt[i].writeTempDynamicConfigFile(nextQuorumCfgSection, "200000000");
            mt[i].start();
            zk[i] = new ZooKeeper("127.0.0.1:" + ports[i][2],
                    ClientBase.CONNECTION_TIMEOUT, this);
        }
                        mt[3] = new MainThread(3, ports[3][2], currentQuorumCfg
                + allServersNext.get(1));
        mt[3].start();
        zk[3] = new ZooKeeper("127.0.0.1:" + ports[3][2],
                ClientBase.CONNECTION_TIMEOUT, this);

        for (int i = 2; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + ports[i][2],
                            CONNECTION_TIMEOUT * 3));
            ReconfigTest.testServerHasConfig(zk[i], allServersNext, null);
        }

        ReconfigTest.testNormalOperation(zk[0], zk[2]);
        ReconfigTest.testNormalOperation(zk[3], zk[1]);
        Assert.assertEquals(nextQuorumCfgSection + "version=200000000",
                ReconfigTest.testServerHasConfig(zk[2], null, null));
        Assert.assertEquals(nextQuorumCfgSection + "version=200000000",
                ReconfigTest.testServerHasConfig(zk[3], null, null));

        for (int i = 0; i < SERVER_COUNT; i++) {
            zk[i].close();
            mt[i].shutdown();
        }
    }

    
    public static int[][] generatePorts(int numServers) {
        int[][] ports = new int[numServers][];
        for (int i = 0; i < numServers; i++) {
            ports[i] = new int[3];
            for (int j = 0; j < 3; j++) {
                ports[i][j] = PortAssignment.unique();
            }
        }
        return ports;
    }

    
    public static StringBuilder generateConfig(int numServers, int[][] ports,
            HashSet<Integer> observerIds) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numServers; i++) {
            String server = "server." + i + "=localhost:" + ports[i][0] + ":"
                    + ports[i][1] + ":"
                    + (observerIds.contains(i) ? "observer" : "participant")
                    + ";localhost:" + ports[i][2];
            sb.append(server + "\n");
        }
        return sb;
    }
}