package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class ACLCountTest extends ZKTestCase{
    private static final Logger LOG = LoggerFactory.getLogger(ACLCountTest.class);
    private static final String HOSTPORT =
        "127.0.0.1:" + PortAssignment.unique();

    
    @Test
    public void testAclCount() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(1000);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        ZooKeeper zk;

        final ArrayList<ACL> CREATOR_ALL_AND_WORLD_READABLE =
          new ArrayList<ACL>() { {
            add(new ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE));
            add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
            add(new ACL(ZooDefs.Perms.READ,ZooDefs.Ids.ANYONE_ID_UNSAFE));
            add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.AUTH_IDS));
        }};

        try {
            LOG.info("starting up the zookeeper server .. waiting");
            Assert.assertTrue("waiting for server being up",
                    ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
            zk = ClientBase.createZKClient(HOSTPORT);

            zk.addAuthInfo("digest", "pat:test".getBytes());
            zk.setACL("/", Ids.CREATOR_ALL_ACL, -1);

            String path = "/path";

            try {
              Assert.assertEquals(4,CREATOR_ALL_AND_WORLD_READABLE.size());
            }
            catch (Exception e) {
              LOG.error("Something is fundamentally wrong with ArrayList's add() method. add()ing four times to an empty ArrayList should result in an ArrayList with 4 members.");
              throw e;
            }

            zk.create(path,path.getBytes(),CREATOR_ALL_AND_WORLD_READABLE,CreateMode.PERSISTENT);
            List<ACL> acls = zk.getACL("/path", new Stat());
            Assert.assertEquals(2,acls.size());
        }
        catch (Exception e) {
                    Assert.assertTrue(false);
        }

        f.shutdown();
        zks.shutdown();
    }
}
