package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class NullDataTest extends ClientBase implements StatCallback {
    String snapCount;
    CountDownLatch cn = new CountDownLatch(1);
    
    @Override
    public void setUp() throws Exception {
                snapCount = System.getProperty("zookeeper.snapCount", "1024");
        System.setProperty("zookeeper.snapCount", "10");
        super.setUp();
    }
    
    @Override
    public void tearDown() throws Exception {
        System.setProperty("zookeeper.snapCount", snapCount);
        super.tearDown();
    }
    
    @Test
    public void testNullData() throws IOException, 
        InterruptedException, KeeperException {
        String path = "/SIZE";
        ZooKeeper zk = null;
        zk = createClient();
        try {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        zk.exists(path, false);
            zk.exists(path, false, this , null);
            cn.await(10, TimeUnit.SECONDS);
            Assert.assertSame(0L, cn.getCount());
        } finally {
            if(zk != null)
                zk.close();
        }
        
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {
        cn.countDown();
    }
}
