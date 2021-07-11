package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ReconfigTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;


public class ReconfigRollingRestartCompatibilityTest extends QuorumPeerTestBase {
    private static final String ZOO_CFG_BAK_FILE = "zoo.cfg.bak";

    Map<Integer, Integer> clientPorts = new HashMap<>(5);
    Map<Integer, String> serverAddress = new HashMap<>(5);

    private String generateNewQuorumConfig(int serverCount) {
        StringBuilder sb = new StringBuilder();
        String server;
        for (int i = 0; i < serverCount; i++) {
            clientPorts.put(i, PortAssignment.unique());
            server = "server." + i + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + clientPorts.get(i);
            serverAddress.put(i, server);
            sb.append(server + "\n");
        }
        return sb.toString();
    }

    private String updateExistingQuorumConfig(List<Integer> sidsToAdd, List<Integer> sidsToRemove) {
        StringBuilder sb = new StringBuilder();
        for (Integer sid : sidsToAdd) {
            clientPorts.put(sid, PortAssignment.unique());
            serverAddress.put(sid, "server." + sid + "=localhost:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;localhost:"
                    + clientPorts.get(sid));
        }

        for (Integer sid : sidsToRemove) {
            clientPorts.remove(sid);
            serverAddress.remove(sid);
        }

        for (String server : serverAddress.values()) {
            sb.append(server + "\n");
        }

        return sb.toString();
    }

    @Test(timeout = 60000)
            public void testNoLocalDynamicConfigAndBackupFiles()
            throws InterruptedException, IOException {
        int serverCount = 3;
        String config = generateNewQuorumConfig(serverCount);
        QuorumPeerTestBase.MainThread mt[] = new QuorumPeerTestBase.MainThread[serverCount];
        String[] staticFileContent = new String[serverCount];

        for (int i = 0; i < serverCount; i++) {
            mt[i] = new QuorumPeerTestBase.MainThread(i, clientPorts.get(i),
                    config, false);
            mt[i].start();
        }

        for (int i = 0; i < serverCount; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts.get(i),
                            CONNECTION_TIMEOUT));
            Assert.assertNull("static file backup (zoo.cfg.bak) shouldn't exist!",
                    mt[i].getFileByName(ZOO_CFG_BAK_FILE));
            Assert.assertNull("dynamic configuration file (zoo.cfg.dynamic.*) shouldn't exist!",
                    mt[i].getFileByName(mt[i].getQuorumPeer().getNextDynamicConfigFilename()));
            staticFileContent[i] = Files.readAllLines(mt[i].confFile.toPath(), StandardCharsets.UTF_8).toString();
            Assert.assertTrue("static config file should contain server entry " + serverAddress.get(i),
                    staticFileContent[i].contains(serverAddress.get(i)));
        }

        for (int i = 0; i < serverCount; i++) {
            mt[i].shutdown();
        }
    }

    @Test(timeout = 60000)
                    public void testRollingRestartWithoutMembershipChange() throws Exception {
        int serverCount = 3;
        String config = generateNewQuorumConfig(serverCount);
        List<String> joiningServers = new ArrayList<>();
        QuorumPeerTestBase.MainThread mt[] = new QuorumPeerTestBase.MainThread[serverCount];
        for (int i = 0; i < serverCount; ++i) {
            mt[i] = new QuorumPeerTestBase.MainThread(i, clientPorts.get(i),
                    config, false);
            mt[i].start();
            joiningServers.add(serverAddress.get(i));
        }

        for (int i = 0; i < serverCount; ++i) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts.get(i),
                            CONNECTION_TIMEOUT));
        }

        for (int i = 0; i < serverCount; ++i) {
            mt[i].shutdown();
            mt[i].start();
            verifyQuorumConfig(i, joiningServers, null);
            verifyQuorumMembers(mt[i]);
        }

        for (int i = 0; i < serverCount; i++) {
            mt[i].shutdown();
        }
    }

    @Test(timeout = 90000)
                    public void testRollingRestartWithMembershipChange() throws Exception {
        int serverCount = 3;
        String config = generateNewQuorumConfig(serverCount);
        QuorumPeerTestBase.MainThread mt[] = new QuorumPeerTestBase.MainThread[serverCount];
        List<String> joiningServers = new ArrayList<>();
        for (int i = 0; i < serverCount; ++i) {
            mt[i] = new QuorumPeerTestBase.MainThread(i, clientPorts.get(i),
                    config, false);
            mt[i].start();
            joiningServers.add(serverAddress.get(i));
        }

        for (int i = 0; i < serverCount; ++i) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts.get(i),
                            CONNECTION_TIMEOUT));
        }

        for (int i = 0; i < serverCount; ++i) {
            verifyQuorumConfig(i, joiningServers, null);
            verifyQuorumMembers(mt[i]);
        }

        Map<Integer, String> oldServerAddress = new HashMap<>(serverAddress);
        List<String> newServers = new ArrayList<>(joiningServers);
        config = updateExistingQuorumConfig(Arrays.asList(3, 4), new ArrayList<Integer>());
        newServers.add(serverAddress.get(3)); newServers.add(serverAddress.get(4));
        serverCount = serverAddress.size();
        Assert.assertEquals("Server count should be 5 after config update.", serverCount, 5);

                                        mt = Arrays.copyOf(mt, mt.length + 2);
        for (int i = 3; i < 5; ++i) {
            mt[i] = new QuorumPeerTestBase.MainThread(i, clientPorts.get(i),
                    config, false);
            mt[i].start();
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts.get(i),
                            CONNECTION_TIMEOUT));
            verifyQuorumConfig(i, newServers, null);
            verifyQuorumMembers(mt[i]);
        }

        Set<String> expectedConfigs = new HashSet<>();
        for (String conf : oldServerAddress.values()) {
                        expectedConfigs.add(conf.substring(conf.indexOf('=') + 1));
        }

        for (int i = 0; i < 3; ++i) {
            verifyQuorumConfig(i, joiningServers, null);
            verifyQuorumMembers(mt[i], expectedConfigs);
        }

        for (int i = 0; i < serverCount; ++i) {
            mt[i].shutdown();
        }
    }

        private void verifyQuorumConfig(int sid, List<String> joiningServers, List<String> leavingServers) throws Exception {
        ZooKeeper zk = ClientBase.createZKClient("127.0.0.1:" + clientPorts.get(sid));
        ReconfigTest.testNormalOperation(zk, zk);
        ReconfigTest.testServerHasConfig(zk, joiningServers, leavingServers);
        zk.close();
    }

        private void verifyQuorumMembers(QuorumPeerTestBase.MainThread mt) {
        Set<String> expectedConfigs = new HashSet<>();
        for (String config : serverAddress.values()) {
            expectedConfigs.add(config.substring(config.indexOf('=') + 1));
        }
        verifyQuorumMembers(mt, expectedConfigs);
    }

    private void verifyQuorumMembers(QuorumPeerTestBase.MainThread mt, Set<String> expectedConfigs) {
        Map<Long, QuorumPeer.QuorumServer> members =
                mt.getQuorumPeer().getQuorumVerifier().getAllMembers();

        Assert.assertTrue("Quorum member should not change.",
                members.size() == expectedConfigs.size());

        for (QuorumPeer.QuorumServer qs : members.values()) {
            String actualConfig = qs.toString();
            Assert.assertTrue("Unexpected config " + actualConfig + " found!",
                    expectedConfigs.contains(actualConfig));
        }
    }
}



