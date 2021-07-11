package org.apache.zookeeper.test;

import java.io.File;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QuorumZxidSyncTest extends ZKTestCase {
    QuorumBase qb = new QuorumBase();

    @Before
    public void setUp() throws Exception {
        qb.setUp();
    }

    
    @Test
    public void testBehindLeader() throws Exception {
                ClientBase.waitForServerUp(qb.hostPort, 10000);
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        ZooKeeper zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/0", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/2", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        deleteFiles(qb.s1dir);
        deleteFiles(qb.s2dir);
        deleteFiles(qb.s3dir);
        deleteFiles(qb.s4dir);
        qb.setupServers();
        qb.s1.start();
        qb.s2.start();
        qb.s3.start();
        qb.s4.start();
        Assert.assertTrue("Servers didn't come up", ClientBase.waitForServerUp(qb.hostPort, 10000));
        qb.s5.start();
        String hostPort = "127.0.0.1:" + qb.s5.getClientPort();
        Assert.assertFalse("Servers came up, but shouldn't have since it's ahead of leader",
                ClientBase.waitForServerUp(hostPort, 10000));
    }

    private void deleteFiles(File f) {
        File v = new File(f, "version-2");
        for(File c: v.listFiles()) {
            c.delete();
        }
    }

    
    @Test
    public void testLateLogs() throws Exception {
                ClientBase.waitForServerUp(qb.hostPort, 10000);
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        ZooKeeper zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/0", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/1", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        qb.shutdownServers();
        deleteLogs(qb.s1dir);
        deleteLogs(qb.s2dir);
        deleteLogs(qb.s3dir);
        deleteLogs(qb.s4dir);
        deleteLogs(qb.s5dir);
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        zk.create("/2", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        qb.shutdownServers();
        qb.startServers();
        ClientBase.waitForServerUp(qb.hostPort, 10000);
        zk = new ZooKeeper(qb.hostPort, 10000, new Watcher() {
            public void process(WatchedEvent event) {
            }});
        boolean saw2 = false;
        for(String child: zk.getChildren("/", false)) {
            if (child.equals("2")) {
                saw2 = true;
            }
        }
        zk.close();
        Assert.assertTrue("Didn't see /2 (went back in time)", saw2);
    }

    private void deleteLogs(File f) {
        File v = new File(f, "version-2");
        for(File c: v.listFiles()) {
            if (c.getName().startsWith("log")) {
                c.delete();
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        qb.tearDown();
    }
}
