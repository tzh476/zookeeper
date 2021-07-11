package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SnapshotFormatter;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvalidSnapshotTest extends ZKTestCase{
    private final static Logger LOG = LoggerFactory.getLogger(InvalidSnapshotTest.class);
    private static final String HOSTPORT =
            "127.0.0.1:" + PortAssignment.unique();

    private static final File testData = new File(
            System.getProperty("test.data.dir", "src/test/resources/data"));

    
    @SuppressWarnings("deprecation")
    @Test
    public void testLogFormatter() throws Exception {
        File snapDir = new File(testData, "invalidsnap");
        File logfile = new File(new File(snapDir, "version-2"), "log.274");
        String[] args = {logfile.getCanonicalFile().toString()};
        org.apache.zookeeper.server.LogFormatter.main(args);
    }

    
    @Test
    public void testSnapshotFormatter() throws Exception {
        File snapDir = new File(testData, "invalidsnap");
        File snapfile = new File(new File(snapDir, "version-2"), "snapshot.272");
        String[] args = {snapfile.getCanonicalFile().toString()};
        SnapshotFormatter.main(args);
    }
    
    
    @Test
    public void testSnapshotFormatterWithNull() throws Exception {
        File snapDir = new File(testData, "invalidsnap");
        File snapfile = new File(new File(snapDir, "version-2"), "snapshot.273");
        String[] args = {snapfile.getCanonicalFile().toString()};
        SnapshotFormatter.main(args);
    }
    
    
    @Test
    public void testSnapshot() throws Exception {
        File snapDir = new File(testData, "invalidsnap");
        ZooKeeperServer zks = new ZooKeeperServer(snapDir, snapDir, 3000);
        SyncRequestProcessor.setSnapCount(1000);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        LOG.info("starting up the zookeeper server .. waiting");
        Assert.assertTrue("waiting for server being up",
                ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
                        
            Assert.assertTrue(zk.exists("/9/9/8", false) != null);
        } finally {
            zk.close();
        }
        f.shutdown();
        zks.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown(HOSTPORT,
                           ClientBase.CONNECTION_TIMEOUT));

    }
}
