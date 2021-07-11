package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ReconfigTest;
import org.junit.Assert;
import org.junit.Test;

public class StandaloneDisabledTest extends QuorumPeerTestBase {

    private final int NUM_SERVERS = 5;
    private MainThread peers[];
    private ZooKeeper zkHandles[];
    private ZooKeeperAdmin zkAdminHandles[];
    private int clientPorts[];
    private final int leaderId = 0;
    private final int follower1 = 1;
    private final int follower2 = 2;
    private final int observer1 = 3;
    private final int observer2 = 4;
    private ArrayList<String> serverStrings;
    private ArrayList<String> reconfigServers;

    
    @Test(timeout = 600000)
    public void startSingleServerTest() throws Exception {
        setUpData();

                startServer(leaderId, serverStrings.get(leaderId) + "\n");
        ReconfigTest.testServerHasConfig(zkHandles[leaderId], null, null);
        LOG.info("Initial Configuration:\n"
                 + new String(zkHandles[leaderId].getConfig(this, new Stat())));

                startFollowers();
        testReconfig(leaderId, true, reconfigServers);
        LOG.info("Configuration after adding 2 followers:\n"
                 + new String(zkHandles[leaderId].getConfig(this, new Stat())));

                shutDownServer(leaderId);
        ReconfigTest.testNormalOperation(zkHandles[follower1], zkHandles[follower2]);

                        reconfigServers.clear();
        reconfigServers.add(Integer.toString(follower2));
        try {
            ReconfigTest.reconfig(zkAdminHandles[follower1], null, reconfigServers, null, -1);
            Assert.fail("reconfig completed successfully even though there is no quorum up in new config!");
        } catch (KeeperException.NewConfigNoQuorum e) { }

                        reconfigServers.clear();
        reconfigServers.add(Integer.toString(leaderId));
        reconfigServers.add(Integer.toString(follower1));
        testReconfig(follower2, false, reconfigServers);
        LOG.info("Configuration after removing leader and follower 1:\n"
                + new String(zkHandles[follower2].getConfig(this, new Stat())));

                shutDownServer(follower1);

                reconfigServers.clear();
        reconfigServers.add(Integer.toString(follower2));
        try {
            zkAdminHandles[follower2].reconfigure(null, reconfigServers, null, -1, new Stat());
            Assert.fail("reconfig completed successfully even though there is no quorum up in new config!");
        } catch (KeeperException.BadArgumentsException e) {
                    } catch (Exception e) {
            Assert.fail("Should have been BadArgumentsException!");
        }

                        ArrayList<String> observerStrings = new ArrayList<String>();
        startObservers(observerStrings);
        testReconfig(follower2, true, reconfigServers);         testReconfig(follower2, true, observerStrings);         LOG.info("Configuration after adding two observers:\n"
                + new String(zkHandles[follower2].getConfig(this, new Stat())));

        shutDownData();
    }

    
    private void setUpData() throws Exception {
        ClientBase.setupTestEnv();
        QuorumPeerConfig.setStandaloneEnabled(false);
        QuorumPeerConfig.setReconfigEnabled(true);
        peers = new MainThread[NUM_SERVERS];
        zkHandles = new ZooKeeper[NUM_SERVERS];
        zkAdminHandles = new ZooKeeperAdmin[NUM_SERVERS];
        clientPorts = new int[NUM_SERVERS];
        serverStrings = buildServerStrings();
        reconfigServers = new ArrayList<String>();
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU=");
    }

    
    private void shutDownData() throws Exception {
        for (int i = 0; i < NUM_SERVERS; i++) {
            zkHandles[i].close();
            zkAdminHandles[i].close();
        }
        for (int i = 1; i < NUM_SERVERS; i++) {
            peers[i].shutdown();
        }
    }

    
    private ArrayList<String> buildServerStrings() {
        ArrayList<String> serverStrings = new ArrayList<String>();

        for(int i = 0; i < NUM_SERVERS; i++) {
            clientPorts[i] = PortAssignment.unique();
            String server = "server." + i + "=localhost:" + PortAssignment.unique()
                +":"+PortAssignment.unique() + ":participant;"
                + "localhost:" + clientPorts[i];
            serverStrings.add(server);
        }
        return serverStrings;
    }

    
    private void startServer(int id, String config) throws Exception {
        peers[id] = new MainThread(id, clientPorts[id], config);
        peers[id].start();
        Assert.assertTrue("Server " + id + " is not up",
                          ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[id], CONNECTION_TIMEOUT));
        Assert.assertTrue("Error- Server started in Standalone Mode!",
                peers[id].isQuorumPeerRunning());
        zkHandles[id] = ClientBase.createZKClient("127.0.0.1:" + clientPorts[id]);
        zkAdminHandles[id] = new ZooKeeperAdmin("127.0.0.1:" + clientPorts[id], CONNECTION_TIMEOUT, this);
        zkAdminHandles[id].addAuthInfo("digest", "super:test".getBytes());
        String statCommandOut = FourLetterWordMain.send4LetterWord("127.0.0.1", clientPorts[id], "stat");
        LOG.info(String.format("Started server id %d with config:\n%s\nStat output:\n%s",
                id, config, statCommandOut));
    }

    
    private void shutDownServer(int id) throws Exception {
        peers[id].shutdown();
        ClientBase.waitForServerDown("127.0.0.1:" + clientPorts[id], CONNECTION_TIMEOUT);
        TimeUnit.SECONDS.sleep(25);
    }

    
    private void startFollowers() throws Exception {
        reconfigServers.clear();
        for(int i = 1; i <= 2; i++) {
            String config = serverStrings.get(leaderId) + "\n"
                + serverStrings.get(i)  + "\n"
                + serverStrings.get(i % 2 + 1) + "\n";
            startServer(i, config);
            reconfigServers.add(serverStrings.get(i));
        }
    }
     
    private void startObservers(ArrayList<String> observerStrings) throws Exception {
        reconfigServers.clear();
        for(int i = observer1; i <= observer2; i++) {
            String config = serverStrings.get(follower2) + "\n"
                + serverStrings.get(i) + "\n";
            startServer(i, config);
            reconfigServers.add(serverStrings.get(i));
            observerStrings.add(serverStrings.get(i).replace("participant", "observer"));
        }
    }

    
    private void testReconfig(int id, boolean adding,
                              ArrayList<String> servers) throws Exception {
        if (adding) {
            ReconfigTest.reconfig(zkAdminHandles[id], servers, null, null, -1);
            for (String server : servers) {
                int id2 = Integer.parseInt(server.substring(7, 8));                 ReconfigTest.testNormalOperation(zkHandles[id], zkHandles[id2]);
            }
            ReconfigTest.testServerHasConfig(zkHandles[id], servers, null);
        } else {
            ReconfigTest.reconfig(zkAdminHandles[id], null, servers, null, -1);
            ReconfigTest.testServerHasConfig(zkHandles[id], null, servers);
        }

    }

   
    @Test
    public void startObserver() throws Exception {
        int clientPort = PortAssignment.unique();
        String config = "server." + observer1 + "=localhost:"+ PortAssignment.unique()
            + ":" + clientPort +  ":observer;"
            + "localhost:" + PortAssignment.unique();
        MainThread observer = new MainThread(observer1, clientPort, config);
        observer.start();
        Assert.assertFalse("Observer was able to start by itself!",
                           ClientBase.waitForServerUp("127.0.0.1:" + clientPort, CONNECTION_TIMEOUT));
    }
}
