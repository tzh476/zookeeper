package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnLog.FileTxnIterator;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LoadFromLogTest extends ClientBase {
    private static final int NUM_MESSAGES = 300;
    protected static final Logger LOG = LoggerFactory.getLogger(LoadFromLogTest.class);

        private static final int TRANSACTION_OVERHEAD = 2;
    private static final int TOTAL_TRANSACTIONS = NUM_MESSAGES + TRANSACTION_OVERHEAD;

    @Before
    public void setUp() throws Exception {
        SyncRequestProcessor.setSnapCount(50);
        super.setUp();
    }

    
    @Test
    public void testLoad() throws Exception {
                ZooKeeper zk = createZKClient(hostPort);
        try {
            for (int i = 0; i< NUM_MESSAGES; i++) {
                zk.create("/invalidsnap-" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        stopServer();

                File logDir = new File(tmpDir, FileTxnSnapLog.version + FileTxnSnapLog.VERSION);
        FileTxnLog txnLog = new FileTxnLog(logDir);
        TxnIterator itr = txnLog.read(0);
        
                FileTxnIterator fileItr = (FileTxnIterator) itr;
        long storageSize = fileItr.getStorageSize();
        LOG.info("Txnlog size: " + storageSize + " bytes");
        Assert.assertTrue("Storage size is greater than zero ",
                (storageSize > 0));
        
        long expectedZxid = 0;
        long lastZxid = 0;
        TxnHeader hdr;
        do {
            hdr = itr.getHeader();
            expectedZxid++;
            Assert.assertTrue("not the same transaction. lastZxid=" + lastZxid + ", zxid=" + hdr.getZxid(), lastZxid != hdr.getZxid());
            Assert.assertTrue("excepting next transaction. expected=" + expectedZxid + ", retreived=" + hdr.getZxid(), (hdr.getZxid() == expectedZxid));
            lastZxid = hdr.getZxid();
        }while(itr.next());

        Assert.assertTrue("processed all transactions. " + expectedZxid + " == " + TOTAL_TRANSACTIONS, (expectedZxid == TOTAL_TRANSACTIONS));
    }

    
    @Test
    public void testLoadFailure() throws Exception {
                ZooKeeper zk = createZKClient(hostPort);
        try {
            for (int i = 0; i< NUM_MESSAGES; i++) {
                zk.create("/data-", new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
            }
        } finally {
            zk.close();
        }
        stopServer();

        File logDir = new File(tmpDir, FileTxnSnapLog.version + FileTxnSnapLog.VERSION);
        File[] logFiles = FileTxnLog.getLogFiles(logDir.listFiles(), 0);
                Assert.assertTrue(logFiles.length > NUM_MESSAGES / 100);
                Assert.assertTrue("delete the first log file", logFiles[0].delete());

                long secondStartZxid = Util.getZxidFromName(logFiles[1].getName(), "log");

        FileTxnLog txnLog = new FileTxnLog(logDir);
        TxnIterator itr = txnLog.read(1, false);

                        Assert.assertEquals(secondStartZxid, itr.getHeader().getZxid());

        itr = txnLog.read(secondStartZxid, false);
        Assert.assertEquals(secondStartZxid, itr.getHeader().getZxid());
        Assert.assertTrue(itr.next());

                        long nextZxid = itr.getHeader().getZxid();

        itr = txnLog.read(nextZxid, false);
        Assert.assertEquals(secondStartZxid, itr.getHeader().getZxid());

                        long thirdStartZxid = Util.getZxidFromName(logFiles[2].getName(), "log");
        itr = txnLog.read(thirdStartZxid, false);
        Assert.assertEquals(secondStartZxid, itr.getHeader().getZxid());
        Assert.assertTrue(itr.next());

        nextZxid = itr.getHeader().getZxid();
        itr = txnLog.read(nextZxid, false);
        Assert.assertEquals(secondStartZxid, itr.getHeader().getZxid());
    }

    
    @Test
    public void testRestore() throws Exception {
                ZooKeeper zk = createZKClient(hostPort);
        String lastPath = null;
        try {
            zk.create("/invalidsnap", new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            for (int i = 0; i < NUM_MESSAGES; i++) {
                lastPath = zk.create("/invalidsnap/test-", new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            }
        } finally {
            zk.close();
        }
        String[] tokens = lastPath.split("-");
        String expectedPath = "/invalidsnap/test-"
                + String.format("%010d",
                (Integer.parseInt(tokens[1])) + 1);
        ZooKeeperServer zks = getServer(serverFactory);
        long eZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();
                zks.getZKDatabase().setlastProcessedZxid(
                zks.getZKDatabase().getDataTreeLastProcessedZxid() - 10);
        LOG.info("Set lastProcessedZxid to "
                + zks.getZKDatabase().getDataTreeLastProcessedZxid());
                zks.takeSnapshot();
        zks.shutdown();
        stopServer();

        startServer();
        zks = getServer(serverFactory);
        long fZxid = zks.getZKDatabase().getDataTreeLastProcessedZxid();

                Assert.assertTrue("Restore failed expected zxid=" + eZxid + " found="
                + fZxid, fZxid == eZxid);
        zk = createZKClient(hostPort);

                        String[] children;
        String path;
        try {
            children = zk.getChildren("/invalidsnap", false).toArray(
                    new String[0]);
            path = zk.create("/invalidsnap/test-", new byte[0],
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } finally {
            zk.close();
        }
        LOG.info("Expected " + expectedPath + " found " + path);
        Assert.assertTrue("Error in sequential znode creation expected "
                + expectedPath + " found " + path, path.equals(expectedPath));
        Assert.assertTrue("Unexpected number of children " + children.length
                        + " expected " + NUM_MESSAGES,
                (children.length == NUM_MESSAGES));
    }

    
    @Test
    public void testRestoreWithTransactionErrors() throws Exception {
                ZooKeeper zk = createZKClient(hostPort);
        try {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                try {
                    zk.create("/invaliddir/test-", new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                } catch(NoNodeException e) {
                                    }
            }
        } finally {
            zk.close();
        }

                ZooKeeperServer zks = getServer(serverFactory);
        zks.getZKDatabase().setlastProcessedZxid(
                zks.getZKDatabase().getDataTreeLastProcessedZxid() - 10);
        LOG.info("Set lastProcessedZxid to "
                + zks.getZKDatabase().getDataTreeLastProcessedZxid());

                zks.takeSnapshot();
        zks.shutdown();
        stopServer();

        zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        startServer();
    }

    
    @Test
    public void testDatadirAutocreate() throws Exception {
        stopServer();

        try {
                        System.setProperty(FileTxnSnapLog.ZOOKEEPER_DATADIR_AUTOCREATE, "false");
            tmpDir = createTmpDir();
            startServer();
            Assert.fail("Server should not have started without datadir");
        } catch (IOException e) {
            LOG.info("Server failed to start - correct behavior " + e);
        } finally {
            System.setProperty(FileTxnSnapLog.ZOOKEEPER_DATADIR_AUTOCREATE,
                FileTxnSnapLog.ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT);
        }
    }

    
    @Test
    public void testReloadSnapshotWithMissingParent() throws Exception {
                ZooKeeper zk = createZKClient(hostPort);
        zk.create("/a", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        Stat stat = zk.exists("/a", false);
        long createZxId = stat.getMzxid();
        zk.create("/a/b", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.delete("/a/b", -1);
        zk.delete("/a", -1);
                ZooKeeperServer zks = getServer(serverFactory);
        zks.getZKDatabase().setlastProcessedZxid(createZxId);
        LOG.info("Set lastProcessedZxid to {}", zks.getZKDatabase()
                .getDataTreeLastProcessedZxid());
                zks.takeSnapshot();
        zks.shutdown();
        stopServer();

        startServer();
    }
}
