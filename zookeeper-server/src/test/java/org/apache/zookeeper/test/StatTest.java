package org.apache.zookeeper.test;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class StatTest extends ClientBase {
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

    
    private Stat newStat() {
        Stat stat = new Stat();

        stat.setAversion(100);
        stat.setCtime(100);
        stat.setCversion(100);
        stat.setCzxid(100);
        stat.setDataLength(100);
        stat.setEphemeralOwner(100);
        stat.setMtime(100);
        stat.setMzxid(100);
        stat.setNumChildren(100);
        stat.setPzxid(100);
        stat.setVersion(100);

        return stat;
    }

    @Test
    public void testBasic()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        Stat stat;

        stat = newStat();
        zk.getData(name, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
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

        Stat stat;

        stat = newStat();
        zk.getData(name, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid() + 1, stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(1, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(1, stat.getNumChildren());

        stat = newStat();
        zk.getData(childname, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(zk.getSessionId(), stat.getEphemeralOwner());
        Assert.assertEquals(childname.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
    }

    @Test
    public void testChildren()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        for(int i = 0; i < 10; i++) {
            String childname = name + "/bar" + i;
            zk.create(childname, childname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);

            Stat stat;

            stat = newStat();
            zk.getData(name, false, stat);

            Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
            Assert.assertEquals(stat.getCzxid() + i + 1, stat.getPzxid());
            Assert.assertEquals(stat.getCtime(), stat.getMtime());
            Assert.assertEquals(i + 1, stat.getCversion());
            Assert.assertEquals(0, stat.getVersion());
            Assert.assertEquals(0, stat.getAversion());
            Assert.assertEquals(0, stat.getEphemeralOwner());
            Assert.assertEquals(name.length(), stat.getDataLength());
            Assert.assertEquals(i + 1, stat.getNumChildren());
        }
    }

    @Test
    public void testDataSizeChange()
        throws IOException, KeeperException, InterruptedException
    {
        String name = "/foo";
        zk.create(name, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        Stat stat;

        stat = newStat();
        zk.getData(name, false, stat);

        Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertEquals(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(0, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length(), stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());

        zk.setData(name, (name + name).getBytes(), -1);

        stat = newStat();
        zk.getData(name, false, stat);

        Assert.assertNotSame(stat.getCzxid(), stat.getMzxid());
        Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
        Assert.assertNotSame(stat.getCtime(), stat.getMtime());
        Assert.assertEquals(0, stat.getCversion());
        Assert.assertEquals(1, stat.getVersion());
        Assert.assertEquals(0, stat.getAversion());
        Assert.assertEquals(0, stat.getEphemeralOwner());
        Assert.assertEquals(name.length() * 2, stat.getDataLength());
        Assert.assertEquals(0, stat.getNumChildren());
    }
}
