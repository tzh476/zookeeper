package org.apache.zookeeper.test;

import java.net.InetSocketAddress;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.util.OSMXBean;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NIOConnectionFactoryFdLeakTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(NIOConnectionFactoryFdLeakTest.class);

    @Test
    public void testFileDescriptorLeak() throws Exception {

        OSMXBean osMbean = new OSMXBean();
        if (osMbean.getUnix() != true) {
            LOG.info("Unable to run test on non-unix system");
            return;
        }

        long startFdCount = osMbean.getOpenFileDescriptorCount();
        LOG.info("Start fdcount is: " + startFdCount);

        for (int i = 0; i < 50; ++i) {
            NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
            factory.configure(
                new InetSocketAddress(
                    "127.0.0.1", PortAssignment.unique()), 10);
            factory.start();
            Thread.sleep(100);
            factory.shutdown();
        }

        long endFdCount = osMbean.getOpenFileDescriptorCount();
        LOG.info("End fdcount is: " + endFdCount);

                Assert.assertTrue("Possible fd leakage",
                ((endFdCount - startFdCount) < 50));
    }
}
