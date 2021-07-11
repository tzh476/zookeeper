package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WatcherTest extends ClientBase {
    protected static final Logger LOG = LoggerFactory.getLogger(WatcherTest.class);

    private long timeOfLastWatcherInvocation;

    private final static class MyStatCallback implements StatCallback {
        int rc;
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            ((int[])ctx)[0]++;
            this.rc = rc;
        }
    }

    private class MyWatcher extends CountdownWatcher {
        LinkedBlockingQueue<WatchedEvent> events =
            new LinkedBlockingQueue<WatchedEvent>();

        public void process(WatchedEvent event) {
            super.process(event);
            if (event.getType() != Event.EventType.None) {
                timeOfLastWatcherInvocation = System.currentTimeMillis();
                try {
                    events.put(event);
                } catch (InterruptedException e) {
                    LOG.warn("ignoring interrupt during event.put");
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
                        System.setProperty(ZKClientConfig.DISABLE_AUTO_WATCH_RESET, "false");
    }

    
    @Test
    public void testWatcherCorrectness()
        throws IOException, InterruptedException, KeeperException
    {
        ZooKeeper zk = null;
        try {
            MyWatcher watcher = new MyWatcher();
            zk = createClient(watcher, hostPort);

            StatCallback scb = new StatCallback() {
                public void processResult(int rc, String path, Object ctx,
                        Stat stat) {
                                    }
            };
            VoidCallback vcb = new VoidCallback() {
                public void processResult(int rc, String path, Object ctx) {
                                    }
            };

            String names[] = new String[10];
            for (int i = 0; i < names.length; i++) {
                String name = zk.create("/tc-", "initialvalue".getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                names[i] = name;

                Stat stat = new Stat();
                zk.getData(name, watcher, stat);
                zk.setData(name, "new".getBytes(), stat.getVersion(), scb, null);
                stat = zk.exists(name, watcher);
                zk.delete(name, stat.getVersion(), vcb, null);
            }

            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                WatchedEvent event = watcher.events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals(name, event.getPath());
                Assert.assertEquals(Event.EventType.NodeDataChanged, event.getType());
                Assert.assertEquals(Event.KeeperState.SyncConnected, event.getState());
                event = watcher.events.poll(10, TimeUnit.SECONDS);
                Assert.assertEquals(name, event.getPath());
                Assert.assertEquals(Event.EventType.NodeDeleted, event.getType());
                Assert.assertEquals(Event.KeeperState.SyncConnected, event.getState());
            }
        } finally {
            if (zk != null) {
                zk.close();
            }
        }
    }
    
    @Test
    public void testWatcherDisconnectOnClose() 
        throws IOException, InterruptedException, KeeperException 
    {
        ZooKeeper zk = null;
        try {
            final BlockingQueue<WatchedEvent> queue = new LinkedBlockingQueue<>();
            
            MyWatcher connWatcher = new MyWatcher();
            
            Watcher watcher = new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                    try {
                        queue.put(event);
                    } catch (InterruptedException e) {
                                            }
                }
                
            };
            
            zk = createClient(connWatcher, hostPort);
    
            StatCallback scb = new StatCallback() {
                public void processResult(int rc, String path, Object ctx,
                        Stat stat) {
                                    }
            };
            
                        zk.exists("/missing", watcher, scb, null);
            
                        zk.close();
            
            
            WatchedEvent event = queue.poll(10, TimeUnit.SECONDS);
            
            Assert.assertNotNull("No watch event was received after closing the Zookeeper client. A 'Closed' event should have occurred", event);
            Assert.assertEquals("Closed events are not generated by the server, and so should have a type of 'None'", Event.EventType.None, event.getType());
            Assert.assertEquals("A 'Closed' event was expected as the Zookeeper client was closed without altering the node it was watching", Event.KeeperState.Closed, event.getState());
        } finally {
            if (zk != null) {
                zk.close();
            }
        }

    }

    @Test
    public void testWatcherCount()
    throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk1 = null, zk2 = null;
        try {
            MyWatcher w1 = new MyWatcher();
            zk1 = createClient(w1, hostPort);

            MyWatcher w2 = new MyWatcher();
            zk2 = createClient(w2, hostPort);

            Stat stat = new Stat();
            zk1.create("/watch-count-test", "value".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zk1.create("/watch-count-test-2", "value".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            zk1.getData("/watch-count-test", w1, stat);
            zk1.getData("/watch-count-test-2", w1, stat);
            zk2.getData("/watch-count-test", w2, stat);

            Assert.assertEquals(ClientBase.getServer(serverFactory)
                    .getZKDatabase().getDataTree().getWatchCount(), 3);

        } finally {
            if(zk1 != null) {
                zk1.close();
            }
            if(zk2 != null) {
                zk2.close();
            }
        }

    }

    final static int COUNT = 100;
    
    @Test
    public void testWatchAutoResetWithPending() throws Exception {
       MyWatcher watches[] = new MyWatcher[COUNT];
       MyStatCallback cbs[] = new MyStatCallback[COUNT];
       MyWatcher watcher = new MyWatcher();
       int count[] = new int[1];
       TestableZooKeeper zk = createClient(watcher, hostPort, 6000);
       ZooKeeper zk2 = createClient(watcher, hostPort, 5000);
       zk2.create("/test", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
       for(int i = 0; i < COUNT/2; i++) {
           watches[i] = new MyWatcher();
           cbs[i] = new MyStatCallback();
           zk.exists("/test", watches[i], cbs[i], count);
       }
       zk.exists("/test", false);
       Assert.assertTrue("Failed to pause the connection!", zk.pauseCnxn(3000));
       zk2.close();
       stopServer();
       watches[0].waitForDisconnected(60000);
       for(int i = COUNT/2; i < COUNT; i++) {
           watches[i] = new MyWatcher();
           cbs[i] = new MyStatCallback();
           zk.exists("/test", watches[i], cbs[i], count);
       }
       startServer();
       watches[COUNT/2-1].waitForConnected(60000);
       Assert.assertEquals(null, zk.exists("/test", false));
       waitForAllWatchers();
       for(int i = 0; i < COUNT/2; i++) {
           Assert.assertEquals("For " + i, 1, watches[i].events.size());
       }
       for(int i = COUNT/2; i < COUNT; i++) {
           if (cbs[i].rc == 0) {
               Assert.assertEquals("For " +i, 1, watches[i].events.size());
           } else {
               Assert.assertEquals("For " +i, 0, watches[i].events.size());
           }
       }
       Assert.assertEquals(COUNT, count[0]);
       zk.close();
    }

    
    private void waitForAllWatchers() throws Exception {
      timeOfLastWatcherInvocation = System.currentTimeMillis();
      while (System.currentTimeMillis() - timeOfLastWatcherInvocation < 1000) {
        Thread.sleep(1000);
      }
    }

    final int TIMEOUT = 5000;

    @Test
    public void testWatcherAutoResetWithGlobal() throws Exception {
        ZooKeeper zk = null;
        MyWatcher watcher = new MyWatcher();
        zk = createClient(watcher, hostPort, TIMEOUT);
        testWatcherAutoReset(zk, watcher, watcher);
        zk.close();
    }

    @Test
    public void testWatcherAutoResetWithLocal() throws Exception {
        ZooKeeper zk = null;
        MyWatcher watcher = new MyWatcher();
        zk = createClient(watcher, hostPort, TIMEOUT);
        testWatcherAutoReset(zk, watcher, new MyWatcher());
        zk.close();
    }

    @Test
    public void testWatcherAutoResetDisabledWithGlobal() throws Exception {
        
        System.setProperty(ZKClientConfig.DISABLE_AUTO_WATCH_RESET, "true");
        testWatcherAutoResetWithGlobal();
    }

    @Test
    public void testWatcherAutoResetDisabledWithLocal() throws Exception {
        System.setProperty(ZKClientConfig.DISABLE_AUTO_WATCH_RESET, "true");
        testWatcherAutoResetWithLocal();
    }

    private void testWatcherAutoReset(ZooKeeper zk, MyWatcher globalWatcher,
            MyWatcher localWatcher) throws Exception {
        boolean isGlobal = (localWatcher == globalWatcher);
                zk.create("/watchtest", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.create("/watchtest/child", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        if (isGlobal) {
            zk.getChildren("/watchtest", true);
            zk.getData("/watchtest/child", true, new Stat());
            zk.exists("/watchtest/child2", true);
        } else {
            zk.getChildren("/watchtest", localWatcher);
            zk.getData("/watchtest/child", localWatcher, new Stat());
            zk.exists("/watchtest/child2", localWatcher);
        }

        Assert.assertTrue(localWatcher.events.isEmpty());

        stopServer();
        globalWatcher.waitForDisconnected(3000);
        localWatcher.waitForDisconnected(500);
        startServer();
        globalWatcher.waitForConnected(3000);
        boolean disableAutoWatchReset = zk.getClientConfig().getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET);
        if (!isGlobal && !disableAutoWatchReset) {
            localWatcher.waitForConnected(500);
        }

        Assert.assertTrue(localWatcher.events.isEmpty());
        zk.setData("/watchtest/child", new byte[1], -1);
        zk.create("/watchtest/child2", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        WatchedEvent e;
        if (!disableAutoWatchReset) {
            e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertEquals(e.getPath(), EventType.NodeDataChanged, e.getType());
            Assert.assertEquals("/watchtest/child", e.getPath());
        } else {
                                }

        if (!disableAutoWatchReset) {
            e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                                    Assert.assertEquals(EventType.NodeCreated, e.getType());
            Assert.assertEquals("/watchtest/child2", e.getPath());
        } else {
                                }

        if (!disableAutoWatchReset) {
            e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
            Assert.assertEquals("/watchtest", e.getPath());
        } else {
                                }

        Assert.assertTrue(localWatcher.events.isEmpty());         stopServer();
        globalWatcher.waitForDisconnected(TIMEOUT);
        try {
            try {
                localWatcher.waitForDisconnected(500);
                if (!isGlobal && !disableAutoWatchReset) {
                    Assert.fail("Got an event when I shouldn't have");
                }
            } catch(TimeoutException toe) {
                if (disableAutoWatchReset) {
                    Assert.fail("Didn't get an event when I should have");
                }
                            }
        } catch (Exception e1) {
            LOG.error("bad", e1);
            throw new RuntimeException(e1);
        }
        startServer();
        globalWatcher.waitForConnected(TIMEOUT);

        if (isGlobal) {
            zk.getChildren("/watchtest", true);
            zk.getData("/watchtest/child", true, new Stat());
            zk.exists("/watchtest/child2", true);
        } else {
            zk.getChildren("/watchtest", localWatcher);
            zk.getData("/watchtest/child", localWatcher, new Stat());
            zk.exists("/watchtest/child2", localWatcher);
        }

                        zk.delete("/watchtest/child2", -1);

        e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(EventType.NodeDeleted, e.getType());
        Assert.assertEquals("/watchtest/child2", e.getPath());

        e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        Assert.assertEquals(EventType.NodeChildrenChanged, e.getType());
        Assert.assertEquals("/watchtest", e.getPath());

        Assert.assertTrue(localWatcher.events.isEmpty());

        stopServer();
        globalWatcher.waitForDisconnected(TIMEOUT);
        localWatcher.waitForDisconnected(500);
        startServer();
        globalWatcher.waitForConnected(TIMEOUT);
        if (!isGlobal && !disableAutoWatchReset) {
            localWatcher.waitForConnected(500);
        }

        zk.delete("/watchtest/child", -1);
        zk.delete("/watchtest", -1);

        if (!disableAutoWatchReset) {
            e = localWatcher.events.poll(TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertEquals(EventType.NodeDeleted, e.getType());
            Assert.assertEquals("/watchtest/child", e.getPath());
        } else {
                                }

                Thread.sleep(1000);
        Assert.assertTrue(localWatcher.events.isEmpty());
    }

}
