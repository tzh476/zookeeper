package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperServerTest extends ZKTestCase {
    @Test
    public void testSortDataDirAscending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", true);

        Assert.assertEquals(orig[2], filelist.get(0));
        Assert.assertEquals(orig[3], filelist.get(1));
        Assert.assertEquals(orig[0], filelist.get(2));
        Assert.assertEquals(orig[1], filelist.get(3));
        Assert.assertEquals(orig[4], filelist.get(4));
    }

    @Test
    public void testSortDataDirDescending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", false);

        Assert.assertEquals(orig[4], filelist.get(0));
        Assert.assertEquals(orig[1], filelist.get(1));
        Assert.assertEquals(orig[0], filelist.get(2));
        Assert.assertEquals(orig[3], filelist.get(3));
        Assert.assertEquals(orig[2], filelist.get(4));
    }

    @Test
    public void testGetLogFiles() {
        File[] files = new File[5];

        files[0] = new File("log.10027c6de");
        files[1] = new File("log.10027c6df");
        files[2] = new File("snapshot.10027c6dd");
        files[3] = new File("log.10027c6dc");
        files[4] = new File("log.20027c6dc");

        File[] orig = files.clone();

        File[] filelist =
                FileTxnLog.getLogFiles(files,
                Long.parseLong("10027c6de", 16));

        Assert.assertEquals(3, filelist.length);
        Assert.assertEquals(orig[0], filelist[0]);
        Assert.assertEquals(orig[1], filelist[1]);
        Assert.assertEquals(orig[4], filelist[2]);
    }

    @Test
    public void testForceSyncDefaultEnabled() {
        File file = new File("foo.10027c6de");
        FileTxnLog log = new FileTxnLog(file);
        Assert.assertTrue(log.isForceSync());
    }

    @Test
    public void testForceSyncDefaultDisabled() {
        try {
            File file = new File("foo.10027c6de");
            System.setProperty("zookeeper.forceSync","no");
            FileTxnLog log = new FileTxnLog(file);
            Assert.assertFalse(log.isForceSync());
        }
        finally {
                        System.setProperty("zookeeper.forceSync","yes");
        }
    }

    @Test
    public void testInvalidSnapshot() {
        File f = null;
        File tmpFileDir = null;
        try {
            tmpFileDir = ClientBase.createTmpDir();
            f = new File(tmpFileDir, "snapshot.0");
            if (!f.exists()) {
                f.createNewFile();
            }
            Assert.assertFalse("Snapshot file size is greater than 9 bytes", Util.isValidSnapshot(f));
            Assert.assertTrue("Can't delete file", f.delete());
        } catch (IOException e) {
        } finally {
            if (null != tmpFileDir) {
                ClientBase.recursiveDelete(tmpFileDir);
            }
        }
    }
}
