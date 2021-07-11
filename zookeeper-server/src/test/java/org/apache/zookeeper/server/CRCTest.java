package org.apache.zookeeper.server;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRCTest extends ZKTestCase{
    private static final Logger LOG = LoggerFactory.getLogger(CRCTest.class);

    private static final String HOSTPORT =
        "127.0.0.1:" + PortAssignment.unique();
    
    private void corruptFile(File file) throws IOException {
                RandomAccessFile raf  = new RandomAccessFile(file, "rw");
        byte[] b = "mahadev".getBytes();
        long writeLen = 500L;
        raf.seek(writeLen);
                raf.write(b);
        raf.close();
    }

    
    private boolean getCheckSum(FileSnap snap, File snapFile) throws IOException {
        DataTree dt = new DataTree();
        Map<Long, Integer> sessions = new ConcurrentHashMap<Long, Integer>();
        InputStream snapIS = new BufferedInputStream(new FileInputStream(
                snapFile));
        CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32());
        InputArchive ia = BinaryInputArchive.getArchive(crcIn);
        try {
            snap.deserialize(dt, sessions, ia);
        } catch (IOException ie) {
                                                            snapIS.close();
            crcIn.close();
            throw ie;
        }

        long checksum = crcIn.getChecksum().getValue();
        long val = ia.readLong("val");
        snapIS.close();
        crcIn.close();
        return (val != checksum);
    }

    
    @Test
    public void testChecksums() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(150);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        LOG.info("starting up the zookeeper server .. waiting");
        Assert.assertTrue("waiting for server being up",
                ClientBase.waitForServerUp(HOSTPORT,CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
            for (int i =0; i < 2000; i++) {
                zk.create("/crctest- " + i , ("/crctest- " + i).getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        f.shutdown();
        zks.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown(HOSTPORT,
                           ClientBase.CONNECTION_TIMEOUT));

        File versionDir = new File(tmpDir, "version-2");
        File[] list = versionDir.listFiles();
                        File snapFile = null;
        File logFile = null;
        for (File file: list) {
            LOG.info("file is " + file);
            if (file.getName().startsWith("log")) {
                logFile = file;
                corruptFile(logFile);
            }
        }
        FileTxnLog flog = new FileTxnLog(versionDir);
        TxnIterator itr = flog.read(1);
                try {
            while (itr.next()) {
            }
            Assert.assertTrue(false);
        } catch(IOException ie) {
            LOG.info("crc corruption", ie);
        }
        itr.close();
                FileSnap snap = new FileSnap(versionDir);
        List<File> snapFiles = snap.findNRecentSnapshots(2);
        snapFile = snapFiles.get(0);
        corruptFile(snapFile);
        boolean cfile = false;
        try {
            cfile = getCheckSum(snap, snapFile);
        } catch(IOException ie) {
                                                snapFile = snapFiles.get(1);
            corruptFile(snapFile);
            cfile = getCheckSum(snap, snapFile);
        }
        Assert.assertTrue(cfile);
   }
}
