package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Test;


public class StandaloneTest extends QuorumPeerTestBase implements Watcher{
    protected static final Logger LOG =
        LoggerFactory.getLogger(StandaloneTest.class);

    @Before
    public void setup() {
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU=");
        QuorumPeerConfig.setReconfigEnabled(true);
    }

    
    @Test
    public void testNoDynamicConfig() throws Exception {
        ClientBase.setupTestEnv();
        final int CLIENT_PORT = PortAssignment.unique();

        MainThread mt = new MainThread(
                MainThread.UNSET_MYID, CLIENT_PORT, "", false);
        verifyStandalone(mt, CLIENT_PORT);
    }

    
    @Test
    public void testClientPortInDynamicFile() throws Exception {
        ClientBase.setupTestEnv();
        final int CLIENT_PORT = PortAssignment.unique();

        String quorumCfgSection = "server.1=127.0.0.1:" +
                (PortAssignment.unique()) + ":" + (PortAssignment.unique())
                + ":participant;" + CLIENT_PORT + "\n";

        MainThread mt = new MainThread(1, quorumCfgSection);
        verifyStandalone(mt, CLIENT_PORT);
    }

    
    @Test
    public void testClientPortInStaticFile() throws Exception {
        ClientBase.setupTestEnv();
        final int CLIENT_PORT = PortAssignment.unique();

        String quorumCfgSection = "server.1=127.0.0.1:" +
                (PortAssignment.unique()) + ":" + (PortAssignment.unique())
                + ":participant;" + CLIENT_PORT + "\n";

        MainThread mt = new MainThread(1, quorumCfgSection, false);
        verifyStandalone(mt, CLIENT_PORT);
    }

    void verifyStandalone(MainThread mt, int clientPort) throws InterruptedException {
        mt.start();
        try {
            Assert.assertTrue("waiting for server 1 being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPort,
                            CONNECTION_TIMEOUT));
        } finally {
            Assert.assertFalse("Error- MainThread started in Quorum Mode!",
                    mt.isQuorumPeerRunning());
            mt.shutdown();
        }
    }

    
    @Test
    public void testStandaloneReconfigFails() throws Exception {
        ClientBase.setupTestEnv();

        final int CLIENT_PORT = PortAssignment.unique();
        final String HOSTPORT = "127.0.0.1:" + CLIENT_PORT;

        File tmpDir = ClientBase.createTmpDir();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        ServerCnxnFactory f = ServerCnxnFactory.createFactory(CLIENT_PORT, -1);
        f.startup(zks);
        Assert.assertTrue("waiting for server being up ", ClientBase
                .waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));

        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, watcher);
        ZooKeeperAdmin zkAdmin = new ZooKeeperAdmin(HOSTPORT, CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(CONNECTION_TIMEOUT);

        List<String> joiners = new ArrayList<String>();
        joiners.add("server.2=localhost:1234:1235;1236");
                try {
            zkAdmin.addAuthInfo("digest", "super:test".getBytes());
            zkAdmin.reconfigure(joiners, null, null, -1, new Stat());
            Assert.fail("Reconfiguration in standalone should trigger " +
                        "UnimplementedException");
        } catch (KeeperException.UnimplementedException ex) {
                    }
        zk.close();

        zks.shutdown();
        f.shutdown();
        Assert.assertTrue("waiting for server being down ", ClientBase
                .waitForServerDown(HOSTPORT, CONNECTION_TIMEOUT));
    }
}
