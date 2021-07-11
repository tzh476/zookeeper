package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class RepeatStartupTest extends ZKTestCase {

    
    @Test
    public void testFail() throws Exception {
        QuorumBase qb = new QuorumBase();
        qb.setUp();

        System.out.println("Comment: the servers are at " + qb.hostPort);
        ZooKeeper zk = qb.createClient();
        zk.create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        QuorumBase.shutdown(qb.s1);
        QuorumBase.shutdown(qb.s2);
        QuorumBase.shutdown(qb.s3);
        QuorumBase.shutdown(qb.s4);
        QuorumBase.shutdown(qb.s5);
        String hp = qb.hostPort.split(",")[0];
        ZooKeeperServer zks = new ZooKeeperServer(qb.s1.getTxnFactory().getSnapDir(),
                qb.s1.getTxnFactory().getDataDir(), 3000);
        final int PORT = Integer.parseInt(hp.split(":")[1]);
        ServerCnxnFactory factory = ServerCnxnFactory.createFactory(PORT, -1);

        factory.startup(zks);
        System.out.println("Comment: starting factory");
        Assert.assertTrue("waiting for server up",
                   ClientBase.waitForServerUp("127.0.0.1:" + PORT,
                           QuorumTest.CONNECTION_TIMEOUT));
        factory.shutdown();
        zks.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown("127.0.0.1:" + PORT,
                                                QuorumTest.CONNECTION_TIMEOUT));
        System.out.println("Comment: shutting down standalone");
    }
}
