package org.apache.zookeeper.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.Assert;
import org.junit.Test;

public class SaslAuthFailDesignatedClientTest extends ClientBase {
    static {
        System.setProperty("zookeeper.authProvider.1","org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                "MyZookeeperClient");

        try {
            File tmpDir = createTmpDir();
            File saslConfFile = new File(tmpDir, "jaas.conf");
            FileWriter fwriter = new FileWriter(saslConfFile);

            fwriter.write("" +
                "Server {\n" +
                "          org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
                "          user_myuser=\"mypassword\";\n" +
                "};\n" +
                "Client {\n" + 
                "       org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
                "       username=\"myuser\"\n" +
                "       password=\"mypassword\";\n" +
                "};" +
                "MyZookeeperClient {\n" +
                "       org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
                "       username=\"myuser\"\n" +
                "       password=\"wrongpassword\";\n" +
                "};" + "\n");
            fwriter.close();
            System.setProperty("java.security.auth.login.config",saslConfFile.getAbsolutePath());
        }
        catch (IOException e) {
                    }
    }


    @Test
    public void testAuth() throws Exception {
                        CountdownWatcher watcher = new CountdownWatcher();
        TestableZooKeeper zk = new TestableZooKeeper(hostPort, CONNECTION_TIMEOUT, watcher);
        if (!watcher.clientConnected.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        {
            Assert.fail("Unable to connect to server");
        }
        try {
            zk.create("/path1", null, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            Assert.fail("Should have gotten exception.");
        } catch (KeeperException e) {
                        LOG.info("Got exception as expected: " + e);
        }
        finally {
            zk.close();
        }
    }
}
