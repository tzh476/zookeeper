package org.apache.zookeeper.test;

import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET;
import static org.junit.Assert.assertTrue;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumRestartTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumRestartTest.class);
    private QuorumUtil qu;

    @Before
    public void setUp() throws Exception {
        System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, "org.apache.zookeeper.ClientCnxnSocketNetty");
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, "org.apache.zookeeper.server.NettyServerCnxnFactory");

                qu = new QuorumUtil(1, 2);
        qu.startAll();
    }

    
    @Test
    public void testRollingRestart() throws Exception {
        for (int serverToRestart = 1; serverToRestart <= 3; serverToRestart++) {
            LOG.info("***** restarting: " + serverToRestart);
            qu.shutdown(serverToRestart);

            assertTrue(String.format("Timeout during waiting for server %d to go down", serverToRestart),
                    ClientBase.waitForServerDown("127.0.0.1:" + qu.getPeer(serverToRestart).clientPort, ClientBase.CONNECTION_TIMEOUT));

            qu.restart(serverToRestart);

            final String errorMessage = "Not all the quorum members are connected after restarting server " + serverToRestart;
            waitFor(errorMessage, () -> qu.allPeersAreConnected(), 30);

            LOG.info("***** Restart {} succeeded", serverToRestart);
        }
    }

    
    @Test
    public void testRollingRestartBackwards() throws Exception {
        for (int serverToRestart = 3; serverToRestart >= 1; serverToRestart--) {
            LOG.info("***** restarting: " + serverToRestart);
            qu.shutdown(serverToRestart);

            assertTrue(String.format("Timeout during waiting for server %d to go down", serverToRestart),
                    ClientBase.waitForServerDown("127.0.0.1:" + qu.getPeer(serverToRestart).clientPort, ClientBase.CONNECTION_TIMEOUT));

            qu.restart(serverToRestart);

            final String errorMessage = "Not all the quorum members are connected after restarting server " + serverToRestart;
            waitFor(errorMessage, () -> qu.allPeersAreConnected(), 30);

            LOG.info("***** Restart {} succeeded", serverToRestart);
        }
    }

    
    @Test
    public void testRestartingLeaderMultipleTimes() throws Exception {
        for (int restartCount = 1; restartCount <= 3; restartCount++) {
            int leaderId = qu.getLeaderServer();
            LOG.info("***** new leader: " + leaderId);
            qu.shutdown(leaderId);

            assertTrue("Timeout during waiting for current leader to go down",
                    ClientBase.waitForServerDown("127.0.0.1:" + qu.getPeer(leaderId).clientPort, ClientBase.CONNECTION_TIMEOUT));

            String errorMessage = "No new leader was elected";
            waitFor(errorMessage, () -> qu.leaderExists() && qu.getLeaderServer() != leaderId, 30);

            qu.restart(leaderId);

            errorMessage = "Not all the quorum members are connected after restarting the old leader";
            waitFor(errorMessage, () -> qu.allPeersAreConnected(), 30);

            LOG.info("***** Leader Restart {} succeeded", restartCount);
        }
    }

    @After
    public void tearDown() throws Exception {
        qu.shutdownAll();
        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
    }
}
