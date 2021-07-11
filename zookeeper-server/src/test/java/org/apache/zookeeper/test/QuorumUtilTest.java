package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.Set;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QuorumUtilTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(QuorumUtilTest.class);

    
    @Test
    public void validateAllMXBeanAreUnregistered() throws IOException {
        QuorumUtil qU = new QuorumUtil(1);
        LOG.info(">-->> Starting up all servers...");
        qU.startAll();
        LOG.info(">-->> Servers up and running...");

        int leaderIndex = qU.getLeaderServer();
        int firstFollowerIndex = 0;
        int secondFollowerIndex = 0;

        switch (leaderIndex) {
        case 1:
            firstFollowerIndex = 2;
            secondFollowerIndex = 3;
            break;
        case 2:
            firstFollowerIndex = 1;
            secondFollowerIndex = 3;
            break;
        case 3:
            firstFollowerIndex = 1;
            secondFollowerIndex = 2;
            break;

        default:
            Assert.fail("Unexpected leaderIndex value: " + leaderIndex);
            break;
        }

        LOG.info(">-->> Shuting down server [{}]", firstFollowerIndex);
        qU.shutdown(firstFollowerIndex);
        LOG.info(">-->> Shuting down server [{}]", secondFollowerIndex);
        qU.shutdown(secondFollowerIndex);
        LOG.info(">-->> Restarting server [{}]", firstFollowerIndex);
        qU.restart(firstFollowerIndex);
        LOG.info(">-->> Restarting server [{}]", secondFollowerIndex);
        qU.restart(secondFollowerIndex);

        qU.shutdownAll();
        Set<ZKMBeanInfo> pending = MBeanRegistry.getInstance()
                .getRegisteredBeans();
        Assert.assertTrue("The following beans should have been unregistered: "
                + pending, pending.isEmpty());
    }
}
