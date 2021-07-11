package org.apache.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoWatcherException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(ZKParameterized.RunnerFactory.class)
public class RemoveWatchesTest extends ClientBase {
    private static final Logger LOG = LoggerFactory
            .getLogger(RemoveWatchesTest.class);
    private ZooKeeper zk1 = null;
    private ZooKeeper zk2 = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        zk1 = createClient();
        zk2 = createClient();
    }

    @Override
    public void tearDown() throws Exception {
        if (zk1 != null)
            zk1.close();
        if (zk2 != null)
            zk2.close();
        super.tearDown();
    }

    private final boolean useAsync;

    public RemoveWatchesTest(boolean useAsync) {
        this.useAsync = useAsync;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true }, });
    }

    private void removeWatches(ZooKeeper zk, String path, Watcher watcher,
            WatcherType watcherType, boolean local, KeeperException.Code rc)
            throws InterruptedException, KeeperException {
        LOG.info(
                "Sending removeWatches req using zk {} path: {} type: {} watcher: {} ",
                new Object[] { zk, path, watcherType, watcher });
        if (useAsync) {
            MyCallback c1 = new MyCallback(rc.intValue(), path);
            zk.removeWatches(path, watcher, watcherType, local, c1, null);
            Assert.assertTrue("Didn't succeeds removeWatch operation",
                    c1.matches());
            if (KeeperException.Code.OK.intValue() != c1.rc) {
                KeeperException ke = KeeperException
                        .create(KeeperException.Code.get(c1.rc));
                throw ke;
            }
        } else {
            zk.removeWatches(path, watcher, watcherType, local);
        }
    }

    private void removeAllWatches(ZooKeeper zk, String path,
            WatcherType watcherType, boolean local, KeeperException.Code rc)
            throws InterruptedException, KeeperException {
        LOG.info("Sending removeWatches req using zk {} path: {} type: {} ",
                new Object[] { zk, path, watcherType });
        if (useAsync) {
            MyCallback c1 = new MyCallback(rc.intValue(), path);
            zk.removeAllWatches(path, watcherType, local, c1, null);
            Assert.assertTrue("Didn't succeeds removeWatch operation",
                    c1.matches());
            if (KeeperException.Code.OK.intValue() != c1.rc) {
                KeeperException ke = KeeperException
                        .create(KeeperException.Code.get(c1.rc));
                throw ke;
            }
        } else {
            zk.removeAllWatches(path, watcherType, local);
        }
    }

    
    @Test(timeout = 90000)
    public void testRemoveSingleWatcher() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk1.create("/node2", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        MyWatcher w2 = new MyWatcher("/node2", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node2", w2));
        removeWatches(zk2, "/node1", w1, WatcherType.Data, false, Code.OK);
        Assert.assertEquals("Didn't find data watcher", 1,
                zk2.getDataWatches().size());
        Assert.assertEquals("Didn't find data watcher", "/node2",
                zk2.getDataWatches().get(0));
        removeWatches(zk2, "/node2", w2, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher", w2.matches());
                        if (zk1 != null) {
            zk1.close();
            zk1 = null;
        }

        List<EventType> events = w1.getEventsAfterWatchRemoval();
        Assert.assertFalse(
                "Shouldn't get NodeDeletedEvent after watch removal",
                events.contains(EventType.NodeDeleted));
        Assert.assertEquals(
                "Shouldn't get NodeDeletedEvent after watch removal", 0,
                events.size());
    }

    
    @Test(timeout = 90000)
    public void testMultipleDataWatchers() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));
        removeWatches(zk2, "/node1", w2, WatcherType.Data, false, Code.OK);
        Assert.assertEquals("Didn't find data watcher", 1,
                zk2.getDataWatches().size());
        Assert.assertEquals("Didn't find data watcher", "/node1",
                zk2.getDataWatches().get(0));
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher", w2.matches());
                        if (zk1 != null) {
            zk1.close();
            zk1 = null;
        }

        List<EventType> events = w2.getEventsAfterWatchRemoval();
        Assert.assertEquals(
                "Shouldn't get NodeDeletedEvent after watch removal", 0,
                events.size());
    }

    
    @Test(timeout = 90000)
    public void testMultipleChildWatchers() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);
        removeWatches(zk2, "/node1", w2, WatcherType.Children, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
        Assert.assertEquals("Didn't find child watcher", 1, zk2
                .getChildWatches().size());
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w1.matches());
                zk1.create("/node1/node2", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
                int count = 30;
        while (count > 0) {
            if (w1.getEventsAfterWatchRemoval().size() > 0) {
                break;
            }
            count--;
            Thread.sleep(100);
        }
                List<EventType> events = w2.getEventsAfterWatchRemoval();
        Assert.assertEquals("Shouldn't get NodeChildrenChanged event", 0,
                events.size());
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllWatchers() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 2);
        MyWatcher w2 = new MyWatcher("/node1", 2);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        removeWatches(zk2, "/node1", w2, WatcherType.Any, false, Code.OK);
        zk1.create("/node1/child", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        Assert.assertTrue("Didn't remove data watcher", w1.matches());
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllDataWatchers() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);
        removeWatches(zk2, "/node1", w1, WatcherType.Data, false, Code.OK);
        removeWatches(zk2, "/node1", w2, WatcherType.Data, false, Code.OK);
        zk1.create("/node1/child", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        Assert.assertTrue("Didn't remove data watcher", w1.matches());
        Assert.assertTrue("Didn't remove data watcher", w2.matches());
                int count = 10;
        while (count > 0) {
            if (w1.getEventsAfterWatchRemoval().size() > 0
                    && w2.getEventsAfterWatchRemoval().size() > 0) {
                break;
            }
            count--;
            Thread.sleep(1000);
        }
                List<EventType> events = w1.getEventsAfterWatchRemoval();
        Assert.assertEquals("Didn't get NodeChildrenChanged event", 1,
                events.size());
        Assert.assertTrue("Didn't get NodeChildrenChanged event",
                events.contains(EventType.NodeChildrenChanged));
                events = w2.getEventsAfterWatchRemoval();
        Assert.assertEquals("Didn't get NodeChildrenChanged event", 1,
                events.size());
        Assert.assertTrue("Didn't get NodeChildrenChanged event",
                events.contains(EventType.NodeChildrenChanged));
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllChildWatchers() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);
        removeWatches(zk2, "/node1", w1, WatcherType.Children, false, Code.OK);
        removeWatches(zk2, "/node1", w2, WatcherType.Children, false, Code.OK);
        zk1.setData("/node1", "test".getBytes(), -1);
        Assert.assertTrue("Didn't remove child watcher", w1.matches());
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
                int count = 10;
        while (count > 0) {
            if (w1.getEventsAfterWatchRemoval().size() > 0
                    && w2.getEventsAfterWatchRemoval().size() > 0) {
                break;
            }
            count--;
            Thread.sleep(1000);
        }
                List<EventType> events = w1.getEventsAfterWatchRemoval();
        Assert.assertEquals("Didn't get NodeDataChanged event", 1,
                events.size());
        Assert.assertTrue("Didn't get NodeDataChanged event",
                events.contains(EventType.NodeDataChanged));
                events = w2.getEventsAfterWatchRemoval();
        Assert.assertEquals("Didn't get NodeDataChanged event", 1,
                events.size());
        Assert.assertTrue("Didn't get NodeDataChanged event",
                events.contains(EventType.NodeDataChanged));
    }

    
    @Test(timeout = 90000)
    public void testNoWatcherException() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 2);
        MyWatcher w2 = new MyWatcher("/node1", 2);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNull("Didn't set data watches", zk2.exists("/node2", w2));
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);

                MyWatcher w3 = new MyWatcher("/node1", 2);

        try {
            removeWatches(zk2, "/node1", w3, WatcherType.Any, false,
                    Code.NOWATCHER);
            Assert.fail("Should throw exception as given watcher doesn't exists");
        } catch (KeeperException.NoWatcherException nwe) {
                    }
        try {
            removeWatches(zk2, "/node1", w3, WatcherType.Children, false,
                    Code.NOWATCHER);
            Assert.fail("Should throw exception as given watcher doesn't exists");
        } catch (KeeperException.NoWatcherException nwe) {
                    }
        try {
            removeWatches(zk2, "/node1", w3, WatcherType.Data, false,
                    Code.NOWATCHER);
            Assert.fail("Should throw exception as given watcher doesn't exists");
        } catch (KeeperException.NoWatcherException nwe) {
                    }
        try {
            removeWatches(zk2, "/nonexists", w3, WatcherType.Data, false,
                    Code.NOWATCHER);
            Assert.fail("Should throw exception as given watcher doesn't exists");
        } catch (KeeperException.NoWatcherException nwe) {
                    }
    }

    
    @Test(timeout = 90000)
    public void testRemoveAnyDataWatcher() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 1);
        MyWatcher w2 = new MyWatcher("/node1", 2);
                LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));
                LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w2);
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher", w1.matches());
        Assert.assertEquals("Didn't find child watcher", 1, zk2
                .getChildWatches().size());
        Assert.assertEquals("Didn't find data watcher", 1, zk2
                .getDataWatches().size());
        removeWatches(zk2, "/node1", w2, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
    }

    
    @Test(timeout = 90000)
    public void testRemoveAnyChildWatcher() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 2);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w2);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w1);
        removeWatches(zk2, "/node1", w2, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
        Assert.assertEquals("Didn't find child watcher", 1, zk2
                .getChildWatches().size());
        Assert.assertEquals("Didn't find data watcher", 1, zk2
                .getDataWatches().size());
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove watchers", w1.matches());
    }

    
    @Test(timeout = 90000)
    public void testRemoveWatcherWhenNoConnection() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 2);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w1);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w2);
        stopServer();
        removeWatches(zk2, "/node1", w2, WatcherType.Any, true, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
        Assert.assertFalse("Shouldn't remove data watcher", w1.matches());
        try {
            removeWatches(zk2, "/node1", w1, WatcherType.Any, false,
                    Code.CONNECTIONLOSS);
            Assert.fail("Should throw exception as last watch removal requires server connection");
        } catch (KeeperException.ConnectionLossException nwe) {
                    }
        Assert.assertFalse("Shouldn't remove data watcher", w1.matches());

                        removeWatches(zk2, "/node1", w1, WatcherType.Any, true, Code.OK);
        Assert.assertTrue("Didn't remove data watcher", w1.matches());
    }

    
    @Test(timeout = 90000)
    public void testManyPreNodeWatchers() throws Exception {
        int count = 50;
        List<MyWatcher> wList = new ArrayList<MyWatcher>(count);
        MyWatcher w;
        String path = "/node";
                for (int i = 0; i < count; i++) {
            final String nodePath = path + i;
            w = new MyWatcher(nodePath, 1);
            wList.add(w);
            LOG.info("Adding pre node watcher {} on path {}", new Object[] { w,
                    nodePath });
            zk1.exists(nodePath, w);
        }
        Assert.assertEquals("Failed to add watchers!", count, zk1
                .getExistWatches().size());
        for (int i = 0; i < count; i++) {
            final MyWatcher watcher = wList.get(i);
            removeWatches(zk1, path + i, watcher, WatcherType.Data, false,
                    Code.OK);
            Assert.assertTrue("Didn't remove data watcher", watcher.matches());
        }
        Assert.assertEquals("Didn't remove watch references!", 0, zk1
                .getExistWatches().size());
    }

    
    @Test(timeout = 90000)
    public void testManyChildWatchers() throws Exception {
        int count = 50;
        List<MyWatcher> wList = new ArrayList<MyWatcher>(count);
        MyWatcher w;
        String path = "/node";

                for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            zk1.create(nodePath, null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            nodePath += "/";
        }
        for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            w = new MyWatcher(path + i, 1);
            wList.add(w);
            LOG.info("Adding child watcher {} on path {}", new Object[] { w,
                    nodePath });
            zk1.getChildren(nodePath, w);
            nodePath += "/";
        }
        Assert.assertEquals("Failed to add watchers!", count, zk1
                .getChildWatches().size());
        for (int i = 0; i < count; i++) {
            final MyWatcher watcher = wList.get(i);
            removeWatches(zk1, path + i, watcher, WatcherType.Children, false,
                    Code.OK);
            Assert.assertTrue("Didn't remove child watcher", watcher.matches());
        }
        Assert.assertEquals("Didn't remove watch references!", 0, zk1
                .getChildWatches().size());
    }

    
    @Test(timeout = 90000)
    public void testManyDataWatchers() throws Exception {
        int count = 50;
        List<MyWatcher> wList = new ArrayList<MyWatcher>(count);
        MyWatcher w;
        String path = "/node";

                for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            w = new MyWatcher(path + i, 1);
            wList.add(w);
            zk1.create(nodePath, null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("Adding data watcher {} on path {}", new Object[] { w,
                    nodePath });
            zk1.getData(nodePath, w, null);
            nodePath += "/";
        }
        Assert.assertEquals("Failed to add watchers!", count, zk1
                .getDataWatches().size());
        for (int i = 0; i < count; i++) {
            final MyWatcher watcher = wList.get(i);
            removeWatches(zk1, path + i, watcher, WatcherType.Data, false,
                    Code.OK);
            Assert.assertTrue("Didn't remove data watcher", watcher.matches());
        }
        Assert.assertEquals("Didn't remove watch references!", 0, zk1
                .getDataWatches().size());
    }

    
    @Test(timeout = 90000)
    public void testManyWatchersWhenNoConnection() throws Exception {
        int count = 3;
        List<MyWatcher> wList = new ArrayList<MyWatcher>(count);
        MyWatcher w;
        String path = "/node";

                for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            zk1.create(nodePath, null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            nodePath += "/";
        }
        for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            w = new MyWatcher(path + i, 2);
            wList.add(w);
            LOG.info("Adding child watcher {} on path {}", new Object[] { w,
                    nodePath });
            zk1.getChildren(nodePath, w);
            nodePath += "/";
        }
        Assert.assertEquals("Failed to add watchers!", count, zk1
                .getChildWatches().size());

                for (int i = 0; i < count; i++) {
            String nodePath = path + i;
            w = wList.get(i);
            LOG.info("Adding data watcher {} on path {}", new Object[] { w,
                    nodePath });
            zk1.getData(nodePath, w, null);
            nodePath += "/";
        }
        Assert.assertEquals("Failed to add watchers!", count, zk1
                .getDataWatches().size());
        stopServer();
        for (int i = 0; i < count; i++) {
            final MyWatcher watcher = wList.get(i);
            removeWatches(zk1, path + i, watcher, WatcherType.Any, true,
                    Code.OK);
            Assert.assertTrue("Didn't remove watcher", watcher.matches());
        }
        Assert.assertEquals("Didn't remove watch references!", 0, zk1
                .getChildWatches().size());
        Assert.assertEquals("Didn't remove watch references!", 0, zk1
                .getDataWatches().size());
    }

    
    @Test(timeout = 90000)
    public void testChRootRemoveWatcher() throws Exception {
                String chRoot = "/appsX";
        zk1.create("/appsX", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        if (zk1 != null) {
            zk1.close();
        }
        if (zk2 != null) {
            zk2.close();
        }
                zk1 = createClient(this.hostPort + chRoot);
        zk2 = createClient(this.hostPort + chRoot);

        LOG.info("Creating child znode /node1 using chRoot client");
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        MyWatcher w1 = new MyWatcher("/node1", 2);
        MyWatcher w2 = new MyWatcher("/node1", 1);
        LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        zk2.getChildren("/node1", w2);
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        zk2.getChildren("/node1", w1);
        removeWatches(zk2, "/node1", w1, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w1.matches());
        Assert.assertEquals("Didn't find child watcher", 1, zk2
                .getChildWatches().size());
        removeWatches(zk2, "/node1", w2, WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher", w2.matches());
    }

    
    @Test(timeout = 90000)
    public void testNoWatcherServerException()
            throws InterruptedException, IOException, TimeoutException {
        CountdownWatcher watcher = new CountdownWatcher();
        MyZooKeeper zk = new MyZooKeeper(hostPort, CONNECTION_TIMEOUT, watcher);
        boolean nw = false;

        watcher.waitForConnected(CONNECTION_TIMEOUT);

        try {
            zk.removeWatches("/nowatchhere", watcher, WatcherType.Data, false);
        } catch (KeeperException nwe) {
            if (nwe.code().intValue() == Code.NOWATCHER.intValue()) {
                nw = true;
            }
        }

        Assert.assertTrue("Server didn't return NOWATCHER",
                zk.getRemoveWatchesRC() == Code.NOWATCHER.intValue());
        Assert.assertTrue("NoWatcherException didn't happen", nw);
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllNoWatcherException() throws IOException,
            InterruptedException, KeeperException {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            removeAllWatches(zk2, "/node1", WatcherType.Any, false,
                    Code.NOWATCHER);
            Assert.fail("Should throw exception as given watcher doesn't exists");
        } catch (KeeperException.NoWatcherException nwe) {
                    }
    }

    
    @Test(timeout = 30000)
    public void testNullWatcherReference() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            if (useAsync) {
                zk1.removeWatches("/node1", null, WatcherType.Data, false,
                        null, null);
            } else {
                zk1.removeWatches("/node1", null, WatcherType.Data, false);
            }
            Assert.fail("Must throw IllegalArgumentException as watcher is null!");
        } catch (IllegalArgumentException iae) {
                    }
    }

    
    @Test(timeout = 90000)
    public void testRemoveWhenMultipleDataWatchesOnAPath() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final CountDownLatch dataWatchCount = new CountDownLatch(1);
        final CountDownLatch rmWatchCount = new CountDownLatch(1);
        Watcher w1 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.DataWatchRemoved) {
                    rmWatchCount.countDown();
                }
            }
        };
        Watcher w2 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeDataChanged) {
                    dataWatchCount.countDown();
                }
            }
        };
                LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));

        removeWatches(zk2, "/node1", w1, WatcherType.Data, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher",
                rmWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));

        zk1.setData("/node1", "test".getBytes(), -1);
        LOG.info("Waiting for data watchers to be notified");
        Assert.assertTrue("Didn't get data watch notification!",
                dataWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    
    @Test(timeout = 90000)
    public void testRemoveWhenMultipleChildWatchesOnAPath() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final CountDownLatch childWatchCount = new CountDownLatch(1);
        final CountDownLatch rmWatchCount = new CountDownLatch(1);
        Watcher w1 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.ChildWatchRemoved) {
                    rmWatchCount.countDown();
                }
            }
        };
        Watcher w2 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeChildrenChanged) {
                    childWatchCount.countDown();
                }
            }
        };
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w1).size());
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w2).size());

        removeWatches(zk2, "/node1", w1, WatcherType.Children, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher",
                rmWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));

        zk1.create("/node1/node2", null, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        LOG.info("Waiting for child watchers to be notified");
        Assert.assertTrue("Didn't get child watch notification!",
                childWatchCount
                        .await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllDataWatchesOnAPath() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final CountDownLatch dWatchCount = new CountDownLatch(2);
        final CountDownLatch rmWatchCount = new CountDownLatch(2);
        Watcher w1 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case DataWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeDataChanged:
                    dWatchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
        Watcher w2 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case DataWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeDataChanged:
                    dWatchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
                LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));

        Assert.assertTrue("Server session is not a watcher",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Data));
        removeAllWatches(zk2, "/node1", WatcherType.Data, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher",
                rmWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));

        Assert.assertFalse("Server session is still a watcher after removal",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Data));
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllChildWatchesOnAPath() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final CountDownLatch cWatchCount = new CountDownLatch(2);
        final CountDownLatch rmWatchCount = new CountDownLatch(2);
        Watcher w1 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case ChildWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeChildrenChanged:
                    cWatchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
        Watcher w2 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case ChildWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeChildrenChanged:
                    cWatchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w1).size());
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w2).size());

        Assert.assertTrue("Server session is not a watcher",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Children));
        removeAllWatches(zk2, "/node1", WatcherType.Children, false, Code.OK);
        Assert.assertTrue("Didn't remove child watcher",
                rmWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));

        Assert.assertFalse("Server session is still a watcher after removal",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Children));
    }

    
    @Test(timeout = 90000)
    public void testRemoveAllWatchesOnAPath() throws Exception {
        zk1.create("/node1", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final CountDownLatch watchCount = new CountDownLatch(2);
        final CountDownLatch rmWatchCount = new CountDownLatch(4);
        Watcher w1 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case ChildWatchRemoved:
                case DataWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeChildrenChanged:
                case NodeDataChanged:
                    watchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
        Watcher w2 = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                case ChildWatchRemoved:
                case DataWatchRemoved:
                    rmWatchCount.countDown();
                    break;
                case NodeChildrenChanged:
                case NodeDataChanged:
                    watchCount.countDown();
                    break;
                default:
                    break;
                }
            }
        };
                LOG.info("Adding child watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w1).size());
        LOG.info("Adding child watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertEquals("Didn't set child watches", 0,
                zk2.getChildren("/node1", w2).size());

                LOG.info("Adding data watcher {} on path {}", new Object[] { w1,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w1));
        LOG.info("Adding data watcher {} on path {}", new Object[] { w2,
                "/node1" });
        Assert.assertNotNull("Didn't set data watches",
                zk2.exists("/node1", w2));

        Assert.assertTrue("Server session is not a watcher",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Data));
        removeAllWatches(zk2, "/node1", WatcherType.Any, false, Code.OK);
        Assert.assertTrue("Didn't remove data watcher",
                rmWatchCount.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
        Assert.assertFalse("Server session is still a watcher after removal",
                isServerSessionWatcher(zk2.getSessionId(), "/node1",
                WatcherType.Data));
        Assert.assertEquals("Received watch notification after removal!", 2,
                watchCount.getCount());
    }

    
    private class MyZooKeeper extends ZooKeeper {
        class MyWatchManager extends ZKWatchManager {
            public MyWatchManager(boolean disableAutoWatchReset) {
                super(disableAutoWatchReset);
            }

            public int lastrc;

            
            void containsWatcher(String path, Watcher watcher,
                    WatcherType watcherType) throws NoWatcherException {
            }

            
            protected boolean removeWatches(
                Map<String, Set<Watcher>> pathVsWatcher,
                Watcher watcher, String path, boolean local, int rc,
                Set<Watcher> removedWatchers) throws KeeperException {
                lastrc = rc;
                return false;
            }
        }

        public MyZooKeeper(String hp, int timeout, Watcher watcher)
                throws IOException {
            super(hp, timeout, watcher, false);
        }

        private MyWatchManager myWatchManager;

        protected ZKWatchManager defaultWatchManager() {
            myWatchManager = new MyWatchManager(getClientConfig().getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET));
            return myWatchManager;
        }

        public int getRemoveWatchesRC() {
            return myWatchManager.lastrc;
        }
    }

    private class MyWatcher implements Watcher {
        private final String path;
        private String eventPath;
        private CountDownLatch latch;
        private List<EventType> eventsAfterWatchRemoval = new ArrayList<EventType>();
        MyWatcher(String path, int count) {
            this.path = path;
            latch = new CountDownLatch(count);
        }

        public void process(WatchedEvent event) {
            LOG.debug("Event path : {}, eventPath : {}"
                    + new Object[] { path, event.getPath() });
            this.eventPath = event.getPath();
                        if (latch.getCount() == 0) {
                if (event.getType() != EventType.None) {
                    eventsAfterWatchRemoval.add(event.getType());
                }
            }
            if (event.getType() == EventType.ChildWatchRemoved
                    || event.getType() == EventType.DataWatchRemoved) {
                latch.countDown();
            }
        }

        
        public boolean matches() throws InterruptedException {
            if (!latch.await(CONNECTION_TIMEOUT/5, TimeUnit.MILLISECONDS)) {
                LOG.error("Failed waiting to remove the watches");
                return false;
            }
            LOG.debug("Client path : {} eventPath : {}", new Object[] { path,
                    eventPath });
            return path.equals(eventPath);
        }

        public List<EventType> getEventsAfterWatchRemoval() {
            return eventsAfterWatchRemoval;
        }
    }

    private class MyCallback implements AsyncCallback.VoidCallback {
        private final String path;
        private final int rc;
        private String eventPath;
        int eventRc;
        private CountDownLatch latch = new CountDownLatch(1);

        public MyCallback(int rc, String path) {
            this.rc = rc;
            this.path = path;
        }

        @Override
        public void processResult(int rc, String eventPath, Object ctx) {
            System.out.println("latch:" + path + " " + eventPath);
            this.eventPath = eventPath;
            this.eventRc = rc;
            this.latch.countDown();
        }

        
        public boolean matches() throws InterruptedException {
            if (!latch.await(CONNECTION_TIMEOUT/5, TimeUnit.MILLISECONDS)) {
                return false;
            }
            return path.equals(eventPath) && rc == eventRc;
        }
    }

    
    private boolean isServerSessionWatcher(long sessionId, String path,
            WatcherType type) {
        Set<ServerCnxn> cnxns = new HashSet<>();
        CollectionUtils.addAll(cnxns, serverFactory.getConnections().iterator());
        for (ServerCnxn cnxn : cnxns) {
            if (cnxn.getSessionId() == sessionId) {
                return getServer(serverFactory).getZKDatabase().getDataTree()
                        .containsWatcher(path, type, cnxn);
            }
        }
        return false;
    }
}
