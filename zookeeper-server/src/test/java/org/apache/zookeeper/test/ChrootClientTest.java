package org.apache.zookeeper.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

public class ChrootClientTest extends ClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChrootClientTest.class);

    @Override
    public void setUp() throws Exception {
        String hp = hostPort;
        hostPort = hostPort + "/chrootclienttest";

        System.out.println(hostPort);
        super.setUp();

        LOG.info("STARTING " + getTestName());

        ZooKeeper zk = createClient(hp);
        try {
            zk.create("/chrootclienttest", null, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } finally {
            zk.close();
        }
    }
    
    @Test
    public void testPing() throws Exception {
            }
}
