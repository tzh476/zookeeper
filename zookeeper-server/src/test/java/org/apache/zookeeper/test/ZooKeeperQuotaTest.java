package org.apache.zookeeper.test;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperQuotaTest extends ClientBase {

    @Test
    public void testQuota() throws IOException,
        InterruptedException, KeeperException, Exception {
        final ZooKeeper zk = createClient();
        final String path = "/a/b/v";
                zk.setData("/", "some".getBytes(), -1);
        zk.create("/a", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b/v", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zk.create("/a/b/v/d", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        ZooKeeperMain.createQuota(zk, path, 5L, 10);

                String absolutePath = Quotas.quotaZookeeper + path + "/" + Quotas.limitNode;
        byte[] data = zk.getData(absolutePath, false, new Stat());
        StatsTrack st = new StatsTrack(new String(data));
        Assert.assertTrue("bytes are set", st.getBytes() == 5L);
        Assert.assertTrue("num count is set", st.getCount() == 10);

        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode;
        byte[] qdata = zk.getData(statPath, false, new Stat());
        StatsTrack qst = new StatsTrack(new String(qdata));
        Assert.assertTrue("bytes are set", qst.getBytes() == 8L);
        Assert.assertTrue("count is set", qst.getCount() == 2);

                stopServer();
        startServer();
        stopServer();
        startServer();
        ZooKeeperServer server = getServer(serverFactory);
        Assert.assertNotNull("Quota is still set",
            server.getZKDatabase().getDataTree().getMaxPrefixWithQuota(path) != null);
    }
}
