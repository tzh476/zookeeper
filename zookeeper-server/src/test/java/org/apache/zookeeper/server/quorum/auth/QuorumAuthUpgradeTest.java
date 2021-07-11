package org.apache.zookeeper.server.quorum.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase.MainThread;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientTest;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;


public class QuorumAuthUpgradeTest extends QuorumAuthTestBase {
    static {
        String jaasEntries = new String("" + "QuorumServer {\n"
                + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
                + "       user_test=\"mypassword\";\n" + "};\n"
                + "QuorumLearner {\n"
                + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
                + "       username=\"test\"\n"
                + "       password=\"mypassword\";\n" + "};\n");
        setupJaasConfig(jaasEntries);
    }

    @After
    public void tearDown() throws Exception {
        shutdownAll();
    }

    @AfterClass
    public static void cleanup() {
        cleanupJaasConfig();
    }

    
    @Test(timeout = 30000)
    public void testNullAuthLearnerServer() throws Exception {
        Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "false");

        String connectStr = startQuorum(2, authConfigs, 0);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT,
                watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.close();
    }

    
    @Test(timeout = 30000)
    public void testAuthLearnerAgainstNullAuthServer() throws Exception {
        Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");

        String connectStr = startQuorum(2, authConfigs, 1);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT,
                watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.close();
    }

    
    @Test(timeout = 30000)
    public void testAuthLearnerAgainstNoAuthRequiredServer() throws Exception {
        Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");

        String connectStr = startQuorum(2, authConfigs, 2);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT,
                watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.close();
    }

    
    @Test(timeout = 30000)
    public void testAuthLearnerServer() throws Exception {
        Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");
        authConfigs.put(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, "true");
        authConfigs.put(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, "true");

        String connectStr = startQuorum(2, authConfigs, 2);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT,
                watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.close();
    }

    
    @Test(timeout = 90000)
    public void testRollingUpgrade() throws Exception {
                                Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "false");

        String connectStr = startQuorum(3, authConfigs, 0);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT,
                watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);

                        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");
        authConfigs.put(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, "false");
        authConfigs.put(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, "false");
        restartServer(authConfigs, 0, zk, watcher);
        restartServer(authConfigs, 1, zk, watcher);
        restartServer(authConfigs, 2, zk, watcher);

                        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");
        authConfigs.put(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, "true");
        authConfigs.put(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, "false");
        restartServer(authConfigs, 0, zk, watcher);
        restartServer(authConfigs, 1, zk, watcher);
        restartServer(authConfigs, 2, zk, watcher);

                        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");
        authConfigs.put(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, "true");
        authConfigs.put(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, "true");
        restartServer(authConfigs, 0, zk, watcher);
        restartServer(authConfigs, 1, zk, watcher);
        restartServer(authConfigs, 2, zk, watcher);

                                authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "false");
        MainThread m = shutdown(2);
        startServer(m, authConfigs);
        Assert.assertFalse("waiting for server 2 being up", ClientBase
                .waitForServerUp("127.0.0.1:" + m.getClientPort(), 5000));
    }

    private void restartServer(Map<String, String> authConfigs, int index,
            ZooKeeper zk, CountdownWatcher watcher) throws IOException,
                    KeeperException, InterruptedException, TimeoutException {
        LOG.info("Restarting server myid=" + index);
        MainThread m = shutdown(index);
        startServer(m, authConfigs);
        Assert.assertTrue("waiting for server" + index + "being up",
                ClientBase.waitForServerUp("127.0.0.1:" + m.getClientPort(),
                        ClientBase.CONNECTION_TIMEOUT));
        watcher.waitForConnected(ClientTest.CONNECTION_TIMEOUT);
        zk.create("/foo", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
    }
}
