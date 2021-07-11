package org.apache.zookeeper.server;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.RandomAccessFile;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvalidSnapshotTest extends ClientBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(InvalidSnapshotTest.class);

    public InvalidSnapshotTest() {
        SyncRequestProcessor.setSnapCount(100);
    }

    
    @Test
    public void testInvalidSnapshot() throws Exception {
        ZooKeeper zk = createClient();
        try {
            for (int i = 0; i < 2000; i++) {
                zk.create("/invalidsnap-" + i, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        NIOServerCnxnFactory factory = (NIOServerCnxnFactory)serverFactory;
        stopServer();

                File snapFile = factory.zkServer.getTxnLogFactory().findMostRecentSnapshot();
        LOG.info("Corrupting " + snapFile);
        RandomAccessFile raf = new RandomAccessFile(snapFile, "rws");
        raf.setLength(3);
        raf.close();

                startServer();

                zk = createClient();
        try {
            assertTrue("the node should exist",
                    (zk.exists("/invalidsnap-1999", false) != null));
        } finally {
            zk.close();
        }
    }
}
