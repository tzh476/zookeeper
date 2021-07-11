package org.apache.zookeeper.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ChrootAsyncTest extends AsyncOpsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChrootAsyncTest.class);

    @Override
    public void setUp() throws Exception {
        String hp = hostPort;
        hostPort = hostPort + "/chrootasynctest";

        super.setUp();

        LOG.info("Creating client " + getTestName());

        ZooKeeper zk = createClient(hp);
        try {
            zk.create("/chrootasynctest", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } finally {
            zk.close();
        }
    }
}
