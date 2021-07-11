package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class GetChildren2Test extends ClientBase {
    private ZooKeeper zk;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        zk = createClient();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        zk.close();
    }

    @Test
    public void testChild()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        String childname = name + "/bar";
        zk.create(childname, childname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        Stat stat = new Stat();
        List<String> s = zk.getChildren(name, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid() + 1, stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(1, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(1, stat.getNumChildren());
        Assert.assertEquals(s.size(), stat.getNumChildren());

        s = zk.getChildren(childname, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(zk.getSessionId(), stat.getEphemeralOwner());
        Assert.assertEquals(childname.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
        Assert.assertEquals(s.size(), stat.getNumChildren());
    }

    @Test
    public void testChildren()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        List<String> children = new ArrayList<String>();
        List<String> children_s = new ArrayList<String>();

        for (int i = 0; i < 10; i++) {
            String childname = name + "/bar" + i;
            String childname_s = "bar" + i;
            children.add(childname);
            children_s.add(childname_s);
        }

        for(int i = 0; i < children.size(); i++) {
            String childname = children.get(i);
            zk.create(childname, childname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);

            Stat stat = new Stat();
            List<String> s = zk.getChildren(name, false, stat);

            Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
            Assert.assertEquals(stat.getCzxid() + i + 1, stat.getPzxid());
            Assert.assertEquals(stat.getCtime(), stat.getMtime());
            Assert.assertEquals(i + 1, stat.getCversion());
            Assert.assertEquals(0, stat.getVersion());
            Assert.assertEquals(0, stat.getAversion());
            Assert.assertEquals(0, stat.getEphemeralOwner());
            Assert.assertEquals(name.length(), stat.getDataLength());
            Assert.assertEquals(i + 1, stat.getNumChildren());
            Assert.assertEquals(s.size(), stat.getNumChildren());
        }
        List<String> p = zk.getChildren(name, false, null);
        List<String> c_a = children_s;
        List<String> c_b = p;
        Collections.sort(c_a);
        Collections.sort(c_b);
        Assert.assertEquals(c_a.size(), 10);
        Assert.assertEquals(c_a, c_b);
    }
}
