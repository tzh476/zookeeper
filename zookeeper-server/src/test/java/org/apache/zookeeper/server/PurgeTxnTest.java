package org.apache.zookeeper.server;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.data.Stat;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.PurgeTxnLog;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurgeTxnTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeTxnTest.class);
    private static String HOSTPORT = "127.0.0.1:" + PortAssignment.unique();
    private static final int CONNECTION_TIMEOUT = 3000;
    private static final long OP_TIMEOUT_IN_MILLIS = 90000;
    private File tmpDir;

    @After
    public void teardown() {
        if (null != tmpDir) {
            ClientBase.recursiveDelete(tmpDir);
        }
    }

    
    @Test
    public void testPurge() throws Exception {
        tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(100);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        Assert.assertTrue("waiting for server being up ",
                ClientBase.waitForServerUp(HOSTPORT,CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
            for (int i = 0; i< 2000; i++) {
                zk.create("/invalidsnap-" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        f.shutdown();
        zks.getTxnLogFactory().close();
        Assert.assertTrue("waiting for server to shutdown",
                ClientBase.waitForServerDown(HOSTPORT, CONNECTION_TIMEOUT));
                PurgeTxnLog.purge(tmpDir, tmpDir, 3);
        FileTxnSnapLog snaplog = new FileTxnSnapLog(tmpDir, tmpDir);
        List<File> listLogs = snaplog.findNRecentSnapshots(4);
        int numSnaps = 0;
        for (File ff: listLogs) {
            if (ff.getName().startsWith("snapshot")) {
                numSnaps++;
            }
        }
        Assert.assertTrue("exactly 3 snapshots ", (numSnaps == 3));
        snaplog.close();
        zks.shutdown();
    }

    
    @Test
    public void testPurgeWhenLogRollingInProgress() throws Exception {
        tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(30);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        Assert.assertTrue("waiting for server being up ",
                ClientBase.waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT));
        final ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        final CountDownLatch doPurge = new CountDownLatch(1);
        final CountDownLatch purgeFinished = new CountDownLatch(1);
        final AtomicBoolean opFailed = new AtomicBoolean(false);
        new Thread() {
            public void run() {
                try {
                    doPurge.await(OP_TIMEOUT_IN_MILLIS / 2,
                            TimeUnit.MILLISECONDS);
                    PurgeTxnLog.purge(tmpDir, tmpDir, 3);
                } catch (IOException ioe) {
                    LOG.error("Exception when purge", ioe);
                    opFailed.set(true);
                } catch (InterruptedException ie) {
                    LOG.error("Exception when purge", ie);
                    opFailed.set(true);
                } finally {
                    purgeFinished.countDown();
                }
            };
        }.start();
        final int thCount = 3;
        List<String> znodes = manyClientOps(zk, doPurge, thCount,
                "/invalidsnap");
        Assert.assertTrue("Purging is not finished!", purgeFinished.await(
                OP_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS));
        Assert.assertFalse("Purging failed!", opFailed.get());
        for (String znode : znodes) {
            try {
                zk.getData(znode, false, null);
            } catch (Exception ke) {
                LOG.error("Unexpected exception when visiting znode!", ke);
                Assert.fail("Unexpected exception when visiting znode!");
            }
        }
        zk.close();
        f.shutdown();
        zks.shutdown();
        zks.getTxnLogFactory().close();
    }

    
    @Test
    public void testFindNRecentSnapshots() throws Exception {
        int nRecentSnap = 4;         int nRecentCount = 30;
        int offset = 0;

        tmpDir = ClientBase.createTmpDir();
        File version2 = new File(tmpDir.toString(), "version-2");
        Assert.assertTrue("Failed to create version_2 dir:" + version2.toString(),
                version2.mkdir());

                FileTxnSnapLog txnLog = new FileTxnSnapLog(tmpDir, tmpDir);
        List<File> foundSnaps = txnLog.findNRecentSnapshots(1);
        assertEquals(0, foundSnaps.size());

        List<File> expectedNRecentSnapFiles = new ArrayList<File>();
        int counter = offset + (2 * nRecentCount);
        for (int i = 0; i < nRecentCount; i++) {
                        File logFile = new File(version2 + "/log." + Long.toHexString(--counter));
            Assert.assertTrue("Failed to create log File:" + logFile.toString(),
                    logFile.createNewFile());
                        File snapFile = new File(version2 + "/snapshot."
                    + Long.toHexString(--counter));
            Assert.assertTrue("Failed to create snap File:" + snapFile.toString(),
                    snapFile.createNewFile());
                        if(i < nRecentSnap){
                expectedNRecentSnapFiles.add(snapFile);
            }
        }

                        List<File> nRecentSnapFiles = txnLog.findNRecentSnapshots(nRecentSnap);
        Assert.assertEquals("exactly 4 snapshots ", 4,
                nRecentSnapFiles.size());
        expectedNRecentSnapFiles.removeAll(nRecentSnapFiles);
        Assert.assertEquals("Didn't get the recent snap files", 0,
                expectedNRecentSnapFiles.size());

                        nRecentSnapFiles = txnLog.findNRecentSnapshots(nRecentCount + 5);
        assertEquals(nRecentCount, nRecentSnapFiles.size());
        for (File f: nRecentSnapFiles) {
            Assert.assertTrue("findNRecentSnapshots() returned a non-snapshot: " + f.getPath(),
                   (Util.getZxidFromName(f.getName(), "snapshot") != -1));
        }

        txnLog.close();
    }

    
    @Test
    public void testSnapFilesGreaterThanToRetain() throws Exception {
        int nRecentCount = 4;
        int fileAboveRecentCount = 4;
        int fileToPurgeCount = 2;
        AtomicInteger offset = new AtomicInteger(0);
        tmpDir = ClientBase.createTmpDir();
        File version2 = new File(tmpDir.toString(), "version-2");
        Assert.assertTrue("Failed to create version_2 dir:" + version2.toString(),
                version2.mkdir());
        List<File> snapsToPurge = new ArrayList<File>();
        List<File> logsToPurge = new ArrayList<File>();
        List<File> snaps = new ArrayList<File>();
        List<File> logs = new ArrayList<File>();
        List<File> snapsAboveRecentFiles = new ArrayList<File>();
        List<File> logsAboveRecentFiles = new ArrayList<File>();
        createDataDirFiles(offset, fileToPurgeCount, false, version2, snapsToPurge,
                logsToPurge);
        createDataDirFiles(offset, nRecentCount, false, version2, snaps, logs);
        logs.add(logsToPurge.remove(0));         createDataDirFiles(offset, fileAboveRecentCount, false, version2,
                snapsAboveRecentFiles, logsAboveRecentFiles);

        
        logsToPurge.remove(0);

        FileTxnSnapLog txnLog = new FileTxnSnapLog(tmpDir, tmpDir);
        PurgeTxnLog.purgeOlderSnapshots(txnLog, snaps.get(snaps.size() - 1));
        txnLog.close();
        verifyFilesAfterPurge(snapsToPurge, false);
        verifyFilesAfterPurge(logsToPurge, false);
        verifyFilesAfterPurge(snaps, true);
        verifyFilesAfterPurge(logs, true);
        verifyFilesAfterPurge(snapsAboveRecentFiles, true);
        verifyFilesAfterPurge(logsAboveRecentFiles, true);
    }

    
    @Test
    public void testSnapFilesEqualsToRetain() throws Exception {
        internalTestSnapFilesEqualsToRetain(false);
    }

    
    @Test
    public void testSnapFilesEqualsToRetainWithPrecedingLog() throws Exception {
        internalTestSnapFilesEqualsToRetain(true);
    }

    public void internalTestSnapFilesEqualsToRetain(boolean testWithPrecedingLogFile) throws Exception {
        int nRecentCount = 3;
        AtomicInteger offset = new AtomicInteger(0);
        tmpDir = ClientBase.createTmpDir();
        File version2 = new File(tmpDir.toString(), "version-2");
        Assert.assertTrue("Failed to create version_2 dir:" + version2.toString(),
                version2.mkdir());
        List<File> snaps = new ArrayList<File>();
        List<File> logs = new ArrayList<File>();
        createDataDirFiles(offset, nRecentCount, testWithPrecedingLogFile, version2, snaps, logs);

        FileTxnSnapLog txnLog = new FileTxnSnapLog(tmpDir, tmpDir);
        PurgeTxnLog.purgeOlderSnapshots(txnLog, snaps.get(snaps.size() - 1));
        txnLog.close();
        verifyFilesAfterPurge(snaps, true);
        verifyFilesAfterPurge(logs, true);
    }

    
    @Test
    public void testSnapFilesLessThanToRetain() throws Exception {
        int nRecentCount = 4;
        int fileToPurgeCount = 2;
        AtomicInteger offset = new AtomicInteger(0);
        tmpDir = ClientBase.createTmpDir();
        File version2 = new File(tmpDir.toString(), "version-2");
        Assert.assertTrue("Failed to create version_2 dir:" + version2.toString(),
                version2.mkdir());
        List<File> snapsToPurge = new ArrayList<File>();
        List<File> logsToPurge = new ArrayList<File>();
        List<File> snaps = new ArrayList<File>();
        List<File> logs = new ArrayList<File>();
        createDataDirFiles(offset, fileToPurgeCount, false, version2, snapsToPurge,
                logsToPurge);
        createDataDirFiles(offset, nRecentCount, false, version2, snaps, logs);
        logs.add(logsToPurge.remove(0)); 
        
        logsToPurge.remove(0);

        FileTxnSnapLog txnLog = new FileTxnSnapLog(tmpDir, tmpDir);
        PurgeTxnLog.purgeOlderSnapshots(txnLog, snaps.get(snaps.size() - 1));
        txnLog.close();
        verifyFilesAfterPurge(snapsToPurge, false);
        verifyFilesAfterPurge(logsToPurge, false);
        verifyFilesAfterPurge(snaps, true);
        verifyFilesAfterPurge(logs, true);
    }

    
    @Test
    public void testPurgeTxnLogWithDataDir()
            throws Exception {
        tmpDir = ClientBase.createTmpDir();
        File dataDir = new File(tmpDir, "dataDir");
        File dataLogDir = new File(tmpDir, "dataLogDir");

        File dataDirVersion2 = new File(dataDir, "version-2");
        dataDirVersion2.mkdirs();
        File dataLogDirVersion2 = new File(dataLogDir, "version-2");
        dataLogDirVersion2.mkdirs();

                int totalFiles = 20;

                        for (int i = 0; i < totalFiles; i++) {
                        File logFile = new File(dataLogDirVersion2, "log."
                    + Long.toHexString(i));
            logFile.createNewFile();
                        File snapFile = new File(dataDirVersion2, "snapshot."
                    + Long.toHexString(i));
            snapFile.createNewFile();
        }

        int numberOfSnapFilesToKeep = 10;
                String[] args = new String[] { dataLogDir.getAbsolutePath(),
                dataDir.getAbsolutePath(), "-n",
                Integer.toString(numberOfSnapFilesToKeep) };
        PurgeTxnLog.main(args);

        assertEquals(numberOfSnapFilesToKeep, dataDirVersion2.listFiles().length);
                assertEquals(numberOfSnapFilesToKeep, dataLogDirVersion2.listFiles().length);
        ClientBase.recursiveDelete(tmpDir);

    }

    
    @Test
    public void testPurgeTxnLogWithoutDataDir()
            throws Exception {
        tmpDir = ClientBase.createTmpDir();
        File dataDir = new File(tmpDir, "dataDir");
        File dataLogDir = new File(tmpDir, "dataLogDir");

        File dataDirVersion2 = new File(dataDir, "version-2");
        dataDirVersion2.mkdirs();
        File dataLogDirVersion2 = new File(dataLogDir, "version-2");
        dataLogDirVersion2.mkdirs();

                int totalFiles = 20;

                for (int i = 0; i < totalFiles; i++) {
                        File logFile = new File(dataLogDirVersion2, "log."
                    + Long.toHexString(i));
            logFile.createNewFile();
                        File snapFile = new File(dataLogDirVersion2, "snapshot."
                    + Long.toHexString(i));
            snapFile.createNewFile();
        }

        int numberOfSnapFilesToKeep = 10;
                String[] args = new String[] { dataLogDir.getAbsolutePath(), "-n",
                Integer.toString(numberOfSnapFilesToKeep) };
        PurgeTxnLog.main(args);
        assertEquals(numberOfSnapFilesToKeep * 2,                 dataLogDirVersion2.listFiles().length);
        ClientBase.recursiveDelete(tmpDir);

    }

    
    @Test
    public void testPurgeDoesNotDeleteOverlappingLogFile() throws Exception {
                final int SNAP_RETAIN_COUNT = 3;
                final int NUM_ZNODES_PER_SNAPSHOT = 100;
        
        SyncRequestProcessor.setSnapCount(SNAP_RETAIN_COUNT * NUM_ZNODES_PER_SNAPSHOT * 10);

                tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        Assert.assertTrue("waiting for server being up ",
                ClientBase.waitForServerUp(HOSTPORT,CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);

                int unique = 0;
        try {
            
            for (int snapshotCount = 0; snapshotCount < SNAP_RETAIN_COUNT; snapshotCount++) {
                for (int i = 0; i< 100; i++, unique++) {
                    zk.create("/snap-" + unique, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                zks.takeSnapshot();
            }
                        for (int i = 0; i< 100; i++, unique++) {
                zk.create("/snap-" + unique, new byte[0], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }

                f.shutdown();
        zks.getTxnLogFactory().close();
        zks.shutdown();
        Assert.assertTrue("waiting for server to shutdown",
                ClientBase.waitForServerDown(HOSTPORT, CONNECTION_TIMEOUT));

                PurgeTxnLog.purge(tmpDir, tmpDir, SNAP_RETAIN_COUNT);

                zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        zk = ClientBase.createZKClient(HOSTPORT);

        
        final String lastZnode = "/snap-" + (unique - 1);
        final Stat stat = zk.exists(lastZnode, false);
        Assert.assertNotNull("Last znode does not exist: " + lastZnode, stat);

                f.shutdown();
        zks.getTxnLogFactory().close();
        zks.shutdown();
    }

    private File createDataDirLogFile(File version_2, int Zxid) throws IOException {
        File logFile = new File(version_2 + "/log." + Long.toHexString(Zxid));
        Assert.assertTrue("Failed to create log File:" + logFile.toString(),
                logFile.createNewFile());
        return logFile;
    }

    private void createDataDirFiles(AtomicInteger offset, int limit, boolean createPrecedingLogFile,
            File version_2, List<File> snaps, List<File> logs)
            throws IOException {
        int counter = offset.get() + (2 * limit);
        if (createPrecedingLogFile) {
            counter++;
        }
        offset.set(counter);
        for (int i = 0; i < limit; i++) {
                        logs.add(createDataDirLogFile(version_2, --counter));
                        File snapFile = new File(version_2 + "/snapshot."
                    + Long.toHexString(--counter));
            Assert.assertTrue("Failed to create snap File:" + snapFile.toString(),
                    snapFile.createNewFile());
            snaps.add(snapFile);
        }
        if (createPrecedingLogFile) {
            logs.add(createDataDirLogFile(version_2, --counter));
        }
    }

    private void verifyFilesAfterPurge(List<File> logs, boolean exists) {
        for (File file : logs) {
            Assert.assertEquals("After purging, file " + file, exists,
                    file.exists());
        }
    }

    private List<String> manyClientOps(final ZooKeeper zk,
            final CountDownLatch doPurge, int thCount, final String prefix) {
        Thread[] ths = new Thread[thCount];
        final List<String> znodes = Collections
                .synchronizedList(new ArrayList<String>());
        final CountDownLatch finished = new CountDownLatch(thCount);
        for (int indx = 0; indx < thCount; indx++) {
            final String myprefix = prefix + "-" + indx;
            Thread th = new Thread() {
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        try {
                            String mynode = myprefix + "-" + i;
                            znodes.add(mynode);
                            zk.create(mynode, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);
                        } catch (Exception e) {
                            LOG.error("Unexpected exception occurred!", e);
                        }
                        if (i == 200) {
                            doPurge.countDown();
                        }
                    }
                    finished.countDown();
                };
            };
            ths[indx] = th;
        }

        for (Thread thread : ths) {
            thread.start();
        }
        try {
            Assert.assertTrue("ZkClient ops is not finished!",
                    finished.await(OP_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS));
        } catch (InterruptedException ie) {
            LOG.error("Unexpected exception occurred!", ie);
            Assert.fail("Unexpected exception occurred!");
        }
        return znodes;
    }
}
