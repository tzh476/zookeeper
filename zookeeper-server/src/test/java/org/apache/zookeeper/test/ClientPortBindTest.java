package org.apache.zookeeper.test;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Test;

public class ClientPortBindTest extends ZKTestCase{
    protected static final Logger LOG = 
        LoggerFactory.getLogger(ClientPortBindTest.class);

    
    @Test
    public void testBindByAddress() throws Exception {
        String bindAddress = null;
        Enumeration<NetworkInterface> intfs =
            NetworkInterface.getNetworkInterfaces();
                while(intfs.hasMoreElements()) {
            NetworkInterface i = intfs.nextElement();
            try {
                if (i.isLoopback()) {
                  Enumeration<InetAddress> addrs = i.getInetAddresses();
                  while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if(a.isLoopbackAddress()) {
                      bindAddress = a.getHostAddress();
                      break;
                    }
                  }
                }
            } catch (SocketException se) {
                LOG.warn("Couldn't find  loopback interface: " + se.getMessage());
            }
        }
        if (bindAddress == null) {
            LOG.warn("Unable to determine loop back address, skipping test");
            return;
        }
        final int PORT = PortAssignment.unique();

        LOG.info("Using " + bindAddress + " as the bind address");
        final String HOSTPORT = bindAddress + ":" + PORT;
        LOG.info("Using " + HOSTPORT + " as the host/port");


        File tmpDir = ClientBase.createTmpDir();

        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);

        ServerCnxnFactory f = ServerCnxnFactory.createFactory(
                new InetSocketAddress(bindAddress, PORT), -1);
        f.startup(zks);
        LOG.info("starting up the the server, waiting");

        Assert.assertTrue("waiting for server up",
                   ClientBase.waitForServerUp(HOSTPORT,
                                   CONNECTION_TIMEOUT));
        ZooKeeper zk = ClientBase.createZKClient(HOSTPORT);
        try {
            zk.close();
        } finally {
            f.shutdown();
            zks.shutdown();

            Assert.assertTrue("waiting for server down",
                       ClientBase.waitForServerDown(HOSTPORT,
                                                    CONNECTION_TIMEOUT));
        }
    }
}
