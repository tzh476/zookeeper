package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.QuorumRestartTest;
import org.apache.zookeeper.test.QuorumUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumCnxManagerSocketConnectionTimeoutTest extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumRestartTest.class);
    private QuorumUtil qu;

    @Before
    public void setUp() throws Exception {
                qu = new QuorumUtil(1, 2);
        qu.startAll();
    }

    
    @Test
    public void testSocketConnectionTimeoutDuringConnectingToElectionAddress() throws Exception {

        int leaderId = qu.getLeaderServer();

                        final InetSocketAddress leaderElectionAddress =
            qu.getLeaderQuorumPeer().getElectionAddress();
        QuorumCnxManager.setSocketFactory(() -> new SocketStub(leaderElectionAddress));

        qu.shutdown(leaderId);

        assertTrue("Timeout during waiting for current leader to go down",
                   ClientBase.waitForServerDown("127.0.0.1:" + qu.getPeer(leaderId).clientPort,
                                                ClientBase.CONNECTION_TIMEOUT));

        String errorMessage = "No new leader was elected";
        waitFor(errorMessage, () -> qu.leaderExists() && qu.getLeaderServer() != leaderId, 15);
    }

    final class SocketStub extends Socket {

        private final InetSocketAddress addressToTimeout;

        SocketStub(InetSocketAddress addressToTimeout) {
            this.addressToTimeout = addressToTimeout;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            if (addressToTimeout.equals(endpoint)) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    LOG.warn("interrupted SocketStub.connect", e);
                }
                throw new SocketTimeoutException("timeout reached in SocketStub.connect()");
            }

            super.connect(endpoint, timeout);
        }
    }

    @After
    public void tearDown() throws Exception {
        qu.shutdownAll();
        QuorumCnxManager.setSocketFactory(QuorumCnxManager.DEFAULT_SOCKET_FACTORY);
    }

}