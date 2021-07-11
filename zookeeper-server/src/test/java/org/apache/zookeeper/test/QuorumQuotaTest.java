package org.apache.zookeeper.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class QuorumQuotaTest extends QuorumBase {

    @Test
    public void testQuotaWithQuorum() throws Exception {
        ZooKeeper zk = createClient();
        zk.setData("/", "some".getBytes(), -1);
        zk.create("/a", "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        int i = 0;
        for (i=0; i < 300;i++) {
            zk.create("/a/" + i, "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        ZooKeeperMain.createQuota(zk, "/a", 1000L, 5000);
        String statPath = Quotas.quotaZookeeper + "/a"+ "/" + Quotas.statNode;
        byte[] data = zk.getData(statPath, false, new Stat());
        StatsTrack st = new StatsTrack(new String(data));
        Assert.assertTrue("bytes are set", st.getBytes() == 1204L);
        Assert.assertTrue("num count is set", st.getCount() == 301);
        for (i=300; i < 600; i++) {
            zk.create("/a/" + i, "some".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        data = zk.getData(statPath, false, new Stat());
        st = new StatsTrack(new String(data));
        Assert.assertTrue("bytes are set", st.getBytes() == 2404L);
        Assert.assertTrue("num count is set", st.getCount() == 601);
    }
}
