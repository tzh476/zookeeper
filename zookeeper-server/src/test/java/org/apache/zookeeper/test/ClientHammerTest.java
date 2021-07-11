package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

public class ClientHammerTest extends ClientBase {
    protected static final Logger LOG = LoggerFactory.getLogger(ClientHammerTest.class);

    private static final long HAMMERTHREAD_LATENCY = 5;

    private static abstract class HammerThread extends Thread {
        protected final int count;
        protected volatile int current = 0;

        HammerThread(String name, int count) {
            super(name);
            this.count = count;
        }
    }

    private static class BasicHammerThread extends HammerThread {
        private final ZooKeeper zk;
        private final String prefix;

        BasicHammerThread(String name, ZooKeeper zk, String prefix, int count) {
            super(name, count);
            this.zk = zk;
            this.prefix = prefix;
        }

        public void run() {
            byte b[] = new byte[256];
            try {
                for (; current < count; current++) {
                                        Thread.sleep(HAMMERTHREAD_LATENCY);
                    zk.create(prefix + current, b, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (Throwable t) {
                LOG.error("Client create operation Assert.failed", t);
            } finally {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected", e);
                }
            }
        }
    }

    private static class SuperHammerThread extends HammerThread {
        private final ClientHammerTest parent;
        private final String prefix;

        SuperHammerThread(String name, ClientHammerTest parent, String prefix,
                int count)
        {
            super(name, count);
            this.parent = parent;
            this.prefix = prefix;
        }

        public void run() {
            byte b[] = new byte[256];
            try {
                for (; current < count; current++) {
                    ZooKeeper zk = parent.createClient();
                    try {
                        zk.create(prefix + current, b, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } finally {
                        try {
                            zk.close();
                        } catch (InterruptedException e) {
                            LOG.warn("Unexpected", e);
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Client create operation Assert.failed", t);
            }
        }
    }

    
    @Test
    public void testHammerBasic() throws Throwable {
        runHammer(10, 1000);
    }

    public void runHammer(final int threadCount, final int childCount)
        throws Throwable
    {
        try {
            HammerThread[] threads = new HammerThread[threadCount];
            long start = Time.currentElapsedTime();
            for (int i = 0; i < threads.length; i++) {
                ZooKeeper zk = createClient();
                String prefix = "/test-" + i;
                zk.create(prefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                prefix += "/";
                HammerThread thread =
                    new BasicHammerThread("BasicHammerThread-" + i, zk, prefix,
                            childCount);
                thread.start();

                threads[i] = thread;
            }

            verifyHammer(start, threads, childCount);
        } catch (Throwable t) {
            LOG.error("test Assert.failed", t);
            throw t;
        }
    }

    
    @Test
    public void testHammerSuper() throws Throwable {
        try {
            final int threadCount = 5;
            final int childCount = 10;

            HammerThread[] threads = new HammerThread[threadCount];
            long start = Time.currentElapsedTime();
            for (int i = 0; i < threads.length; i++) {
                String prefix = "/test-" + i;
                {
                    ZooKeeper zk = createClient();
                    try {
                        zk.create(prefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } finally {
                        zk.close();
                    }
                }
                prefix += "/";
                HammerThread thread =
                    new SuperHammerThread("SuperHammerThread-" + i, this,
                            prefix, childCount);
                thread.start();

                threads[i] = thread;
            }

            verifyHammer(start, threads, childCount);
        } catch (Throwable t) {
            LOG.error("test Assert.failed", t);
            throw t;
        }
    }

    public void verifyHammer(long start, HammerThread[] threads, int childCount)
        throws IOException, InterruptedException, KeeperException
    {
                LOG.info("Starting check for completed hammers");
        int workingCount = threads.length;
        for (int i = 0; i < 120; i++) {
            Thread.sleep(10000);
            for (HammerThread h : threads) {
                if (!h.isAlive() || h.current == h.count) {
                    workingCount--;
                }
            }
            if (workingCount == 0) {
                break;
            }
            workingCount = threads.length;
        }
        if (workingCount > 0) {
            for (HammerThread h : threads) {
                LOG.warn(h.getName() + " never finished creation, current:"
                        + h.current);
            }
        } else {
            LOG.info("Hammer threads completed creation operations");
        }

        for (HammerThread h : threads) {
            final int safetyFactor = 3;
            verifyThreadTerminated(h,
                    (long)threads.length * (long)childCount
                    * HAMMERTHREAD_LATENCY * (long)safetyFactor);
        }
        LOG.info(new Date() + " Total time "
                + (Time.currentElapsedTime() - start));

        ZooKeeper zk = createClient();
        try {
            LOG.info("******************* Connected to ZooKeeper" + new Date());
            for (int i = 0; i < threads.length; i++) {
                LOG.info("Doing thread: " + i + " " + new Date());
                List<String> children =
                    zk.getChildren("/test-" + i, false);
                Assert.assertEquals(childCount, children.size());
                children = zk.getChildren("/test-" + i, false, null);
                Assert.assertEquals(childCount, children.size());
            }
            for (int i = 0; i < threads.length; i++) {
                List<String> children =
                    zk.getChildren("/test-" + i, false);
                Assert.assertEquals(childCount, children.size());
                children = zk.getChildren("/test-" + i, false, null);
                Assert.assertEquals(childCount, children.size());
            }
        } finally {
            zk.close();
        }
    }
}
