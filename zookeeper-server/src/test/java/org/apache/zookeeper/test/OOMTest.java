package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class OOMTest extends ZKTestCase implements Watcher {
    @Test
    public void testOOM() throws IOException, InterruptedException, KeeperException {
                if (true)
            return;
        File tmpDir = ClientBase.createTmpDir();
                        ArrayList<byte[]> hog = new ArrayList<byte[]>();
        while (true) {
            try {
                hog.add(new byte[1024 * 1024 * 2]);
            } catch (OutOfMemoryError e) {
                hog.remove(0);
                break;
            }
        }
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        final int PORT = PortAssignment.unique();
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        Assert.assertTrue("waiting for server up",
                   ClientBase.waitForServerUp("127.0.0.1:" + PORT,
                                              CONNECTION_TIMEOUT));

        System.err.println("OOM Stage 0");
        utestPrep(PORT);
        System.out.println("Free = " + Runtime.getRuntime().freeMemory()
                + " total = " + Runtime.getRuntime().totalMemory() + " max = "
                + Runtime.getRuntime().maxMemory());
        System.err.println("OOM Stage 1");
        for (int i = 0; i < 1000; i++) {
            System.out.println(i);
            utestExists(PORT);
        }
        System.out.println("Free = " + Runtime.getRuntime().freeMemory()
                + " total = " + Runtime.getRuntime().totalMemory() + " max = "
                + Runtime.getRuntime().maxMemory());
        System.err.println("OOM Stage 2");
        for (int i = 0; i < 1000; i++) {
            System.out.println(i);
            utestGet(PORT);
        }
        System.out.println("Free = " + Runtime.getRuntime().freeMemory()
                + " total = " + Runtime.getRuntime().totalMemory() + " max = "
                + Runtime.getRuntime().maxMemory());
        System.err.println("OOM Stage 3");
        for (int i = 0; i < 1000; i++) {
            System.out.println(i);
            utestChildren(PORT);
        }
        System.out.println("Free = " + Runtime.getRuntime().freeMemory()
                + " total = " + Runtime.getRuntime().totalMemory() + " max = "
                + Runtime.getRuntime().maxMemory());
        hog.get(0)[0] = (byte) 1;

        f.shutdown();
        zks.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown("127.0.0.1:" + PORT,
                                                CONNECTION_TIMEOUT));
    }

    private void utestExists(int port)
        throws IOException, InterruptedException, KeeperException
    {
        ZooKeeper zk =
            new ZooKeeper("127.0.0.1:" + port, CONNECTION_TIMEOUT, this);
        for (int i = 0; i < 10000; i++) {
            zk.exists("/this/path/doesnt_exist!", true);
        }
        zk.close();
    }

    private void utestPrep(int port)
        throws IOException, InterruptedException, KeeperException
    {
        ZooKeeper zk =
            new ZooKeeper("127.0.0.1:" + port, CONNECTION_TIMEOUT, this);
        for (int i = 0; i < 10000; i++) {
            zk.create("/" + i, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.close();
    }

    private void utestGet(int port)
        throws IOException, InterruptedException, KeeperException
    {
        ZooKeeper zk =
            new ZooKeeper("127.0.0.1:" + port, CONNECTION_TIMEOUT, this);
        for (int i = 0; i < 10000; i++) {
            Stat stat = new Stat();
            zk.getData("/" + i, true, stat);
        }
        zk.close();
    }

    private void utestChildren(int port)
        throws IOException, InterruptedException, KeeperException
    {
        ZooKeeper zk =
            new ZooKeeper("127.0.0.1:" + port, CONNECTION_TIMEOUT, this);
        for (int i = 0; i < 10000; i++) {
            zk.getChildren("/" + i, true);
        }
        zk.close();
    }

    
    public void process(WatchedEvent event) {
        System.err.println("Got event " + event.getType() + " "
                + event.getState() + " " + event.getPath());
    }
}
