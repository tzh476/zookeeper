package org.apache.zookeeper.server;

import static org.apache.zookeeper.server.DatadirCleanupManager.PurgeTaskStatus.COMPLETED;
import static org.apache.zookeeper.server.DatadirCleanupManager.PurgeTaskStatus.NOT_STARTED;
import static org.apache.zookeeper.server.DatadirCleanupManager.PurgeTaskStatus.STARTED;

import java.io.File;


import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatadirCleanupManagerTest extends ZKTestCase {

    private DatadirCleanupManager purgeMgr;
    private File snapDir;
    private File dataLogDir;

    @Before
    public void setUp() throws Exception {
        File dataDir = ClientBase.createTmpDir();
        snapDir = dataDir;
        dataLogDir = dataDir;
    }

    @Test
    public void testPurgeTask() throws Exception {
        purgeMgr = new DatadirCleanupManager(snapDir, dataLogDir, 3, 1);
        purgeMgr.start();
        Assert.assertEquals("Data log directory is not set as configured",
                dataLogDir, purgeMgr.getDataLogDir());
        Assert.assertEquals("Snapshot directory is not set as configured",
                snapDir, purgeMgr.getSnapDir());
        Assert.assertEquals("Snapshot retain count is not set as configured",
                3, purgeMgr.getSnapRetainCount());
        Assert.assertEquals("Purge task is not started", STARTED, purgeMgr.getPurgeTaskStatus());
        purgeMgr.shutdown();
        Assert.assertEquals("Purge task is still running after shutdown", COMPLETED,
                purgeMgr.getPurgeTaskStatus());
    }

    @Test
    public void testWithZeroPurgeInterval() throws Exception {
        purgeMgr = new DatadirCleanupManager(snapDir, dataLogDir, 3, 0);
        purgeMgr.start();
        Assert.assertEquals("Purge task is scheduled with zero purge interval", NOT_STARTED,
                purgeMgr.getPurgeTaskStatus());
        purgeMgr.shutdown();
        Assert.assertEquals("Purge task is scheduled with zero purge interval", NOT_STARTED,
                purgeMgr.getPurgeTaskStatus());
    }

    @Test
    public void testWithNegativePurgeInterval() throws Exception {
        purgeMgr = new DatadirCleanupManager(snapDir, dataLogDir, 3, -1);
        purgeMgr.start();
        Assert.assertEquals("Purge task is scheduled with negative purge interval",
                NOT_STARTED, purgeMgr.getPurgeTaskStatus());
        purgeMgr.shutdown();
        Assert.assertEquals("Purge task is scheduled with negative purge interval", NOT_STARTED,
                purgeMgr.getPurgeTaskStatus());
    }

    @After
    public void tearDown() throws Exception {
        if (purgeMgr != null) {
            purgeMgr.shutdown();
        }
    }
}
