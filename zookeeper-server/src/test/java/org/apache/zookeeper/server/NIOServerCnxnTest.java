package org.apache.zookeeper.server;

import java.io.IOException;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.BufferStats;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class NIOServerCnxnTest extends ClientBase {
    private static final Logger LOG = LoggerFactory
                        .getLogger(NIOServerCnxnTest.class);

    
    @Test(timeout = 60000)
    public void testOperationsAfterCnxnClose() throws IOException,
            InterruptedException, KeeperException {
        final ZooKeeper zk = createClient();

        final String path = "/a";
        try {
                        zk.create(path, "test".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            assertNotNull("Didn't create znode:" + path,
                    zk.exists(path, false));
                                    Assert.assertTrue(
                    "Didn't instantiate ServerCnxnFactory with NIOServerCnxnFactory!",
                    serverFactory instanceof NIOServerCnxnFactory);
            Iterable<ServerCnxn> connections = serverFactory.getConnections();
            for (ServerCnxn serverCnxn : connections) {
                serverCnxn.close();
                try {
                    serverCnxn.toString();
                } catch (Exception e) {
                    LOG.error("Exception while getting connection details!", e);
                    Assert.fail("Shouldn't throw exception while "
                            + "getting connection details!");
                }
            }
        } finally {
            zk.close();
        }

    }

    @Test
    public void testClientResponseStatsUpdate() throws IOException, InterruptedException, KeeperException {
        try (ZooKeeper zk = createClient()) {
            BufferStats clientResponseStats = serverFactory.getZooKeeperServer().serverStats().getClientResponseStats();
            assertThat("Last client response size should be initialized with INIT_VALUE",
                    clientResponseStats.getLastBufferSize(), equalTo(BufferStats.INIT_VALUE));

            zk.create("/a", "test".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            assertThat("Last client response size should be greater then zero after client request was performed",
                    clientResponseStats.getLastBufferSize(), greaterThan(0));
        }
    }
}
