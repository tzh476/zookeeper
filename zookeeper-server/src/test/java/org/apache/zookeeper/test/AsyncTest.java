package org.apache.zookeeper.test;


import java.util.LinkedList;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTest extends ZKTestCase
    implements StringCallback, VoidCallback, DataCallback
{
    private static final Logger LOG = LoggerFactory.getLogger(AsyncTest.class);

    private QuorumBase qb = new QuorumBase();

    @Before
    public void setUp() throws Exception {
        qb.setUp();
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("Test clients shutting down");
        qb.tearDown();
    }

    private ZooKeeper createClient() throws Exception,InterruptedException {
        return createClient(qb.hostPort);
    }

    private ZooKeeper createClient(String hp)
        throws Exception, InterruptedException
    {
        ZooKeeper zk = ClientBase.createZKClient(hp);
        return zk;
    }

    LinkedList<Integer> results = new LinkedList<Integer>();
    @Test
    public void testAsync() throws Exception
    {
        ZooKeeper zk = null;
        zk = createClient();
        try {
            zk.addAuthInfo("digest", "ben:passwd".getBytes());
            zk.create("/ben", new byte[0], Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT, this, results);
            zk.create("/ben/2", new byte[0], Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT, this, results);
            zk.delete("/ben", -1, this, results);
            zk.create("/ben2", new byte[0], Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT, this, results);
            zk.getData("/ben2", false, this, results);
            synchronized (results) {
                while (results.size() < 5) {
                    results.wait();
                }
            }
            Assert.assertEquals(0, (int) results.get(0));
            Assert.assertEquals(Code.NOAUTH, Code.get(results.get(1)));
            Assert.assertEquals(0, (int) results.get(2));
            Assert.assertEquals(0, (int) results.get(3));
            Assert.assertEquals(0, (int) results.get(4));
        } finally {
            zk.close();
        }

        zk = createClient();
        try {
            zk.addAuthInfo("digest", "ben:passwd2".getBytes());
            try {
                zk.getData("/ben2", false, new Stat());
                Assert.fail("Should have received a permission error");
            } catch (KeeperException e) {
                Assert.assertEquals(Code.NOAUTH, e.code());
            }
        } finally {
            zk.close();
        }

        zk = createClient();
        try {
            zk.addAuthInfo("digest", "ben:passwd".getBytes());
            zk.getData("/ben2", false, new Stat());
        } finally {
            zk.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx, String name) {
        synchronized(ctx) {
            ((LinkedList<Integer>)ctx).add(rc);
            ctx.notifyAll();
        }
    }

    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx) {
        synchronized(ctx) {
            ((LinkedList<Integer>)ctx).add(rc);
            ctx.notifyAll();
        }
    }

    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx, byte[] data,
            Stat stat) {
        synchronized(ctx) {
            ((LinkedList<Integer>)ctx).add(rc);
            ctx.notifyAll();
        }
    }
}
