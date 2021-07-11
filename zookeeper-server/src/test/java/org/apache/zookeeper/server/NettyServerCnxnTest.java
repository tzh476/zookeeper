package org.apache.zookeeper.server;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.TestByteBufAllocator;
import org.apache.zookeeper.server.quorum.BufferStats;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;


public class NettyServerCnxnTest extends ClientBase {
    private static final Logger LOG = LoggerFactory
            .getLogger(NettyServerCnxnTest.class);

    @Override
    public void setUp() throws Exception {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY,
                "org.apache.zookeeper.server.NettyServerCnxnFactory");
        NettyServerCnxnFactory.setTestAllocator(TestByteBufAllocator.getInstance());
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        NettyServerCnxnFactory.clearTestAllocator();
        TestByteBufAllocator.checkForLeaks();
    }

    
    @Test(timeout = 40000)
    public void testSendCloseSession() throws Exception {
        Assert.assertTrue(
                "Didn't instantiate ServerCnxnFactory with NettyServerCnxnFactory!",
                serverFactory instanceof NettyServerCnxnFactory);

        final ZooKeeper zk = createClient();
        final ZooKeeperServer zkServer = getServer(serverFactory);
        final String path = "/a";
        try {
                        zk.create(path, "test".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
                        Assert.assertNotNull("Didn't create znode:" + path,
                    zk.exists(path, true));
            Assert.assertEquals(1, zkServer.getZKDatabase().getDataTree().getWatchCount());
            Iterable<ServerCnxn> connections = serverFactory.getConnections();
            Assert.assertEquals("Mismatch in number of live connections!", 1,
                    serverFactory.getNumAliveConnections());
            for (ServerCnxn serverCnxn : connections) {
                serverCnxn.sendCloseSession();
            }
            LOG.info("Waiting for the channel disconnected event");
            int timeout = 0;
            while (serverFactory.getNumAliveConnections() != 0) {
                Thread.sleep(1000);
                timeout += 1000;
                if (timeout > CONNECTION_TIMEOUT) {
                    Assert.fail("The number of live connections should be 0");
                }
            }
                        Assert.assertEquals(0, zkServer.getZKDatabase().getDataTree().getWatchCount());
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

            zk.create("/a", "test".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            assertThat("Last client response size should be greater than 0 after client request was performed",
                    clientResponseStats.getLastBufferSize(), greaterThan(0));

            byte[] contents = zk.getData("/a", null, null);
            assertArrayEquals("unexpected data", "test".getBytes(StandardCharsets.UTF_8), contents);
        }
    }

    @Test
    public void testServerSideThrottling() throws IOException, InterruptedException, KeeperException {
        try (ZooKeeper zk = createClient()) {
            BufferStats clientResponseStats = serverFactory.getZooKeeperServer().serverStats().getClientResponseStats();
            assertThat("Last client response size should be initialized with INIT_VALUE",
                    clientResponseStats.getLastBufferSize(), equalTo(BufferStats.INIT_VALUE));

            zk.create("/a", "test".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            assertThat("Last client response size should be greater than 0 after client request was performed",
                    clientResponseStats.getLastBufferSize(), greaterThan(0));

            for (final ServerCnxn cnxn : serverFactory.cnxns) {
                final NettyServerCnxn nettyCnxn = ((NettyServerCnxn) cnxn);
                                nettyCnxn.disableRecv();
                                nettyCnxn.getChannel().eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        nettyCnxn.getChannel().read();
                    }
                }, 1, TimeUnit.SECONDS);

                                nettyCnxn.getChannel().eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        nettyCnxn.enableRecv();
                    }
                }, 2, TimeUnit.SECONDS);
            }

            byte[] contents = zk.getData("/a", null, null);
            assertArrayEquals("unexpected data", "test".getBytes(StandardCharsets.UTF_8), contents);

                                    for (final ServerCnxn cnxn : serverFactory.cnxns) {
                final NettyServerCnxn nettyCnxn = ((NettyServerCnxn) cnxn);
                                nettyCnxn.disableRecv();
                                nettyCnxn.getChannel().eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        nettyCnxn.enableRecv();
                    }
                }, 2, TimeUnit.SECONDS);
            }

            contents = zk.getData("/a", null, null);
            assertArrayEquals("unexpected data", "test".getBytes(StandardCharsets.UTF_8), contents);
        }
    }
}
