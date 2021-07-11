package org.apache.zookeeper.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.Assert;
import org.junit.Test;

public class SaslAuthMissingClientConfigTest extends ClientBase {
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
                "};\n");
            fwriter.close();
            System.setProperty("java.security.auth.login.config",saslConfFile.getAbsolutePath());
        }
        catch (IOException e) {
                    }
    }

    @Test
    public void testAuth() throws Exception {
        ZooKeeper zk = createClient();
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
