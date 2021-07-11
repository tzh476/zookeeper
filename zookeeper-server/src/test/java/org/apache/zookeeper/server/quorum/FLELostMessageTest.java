package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.FastLeaderElection;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FLELostMessageTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(FLELostMessageTest.class);

    int count;
    HashMap<Long,QuorumServer> peers;
    File tmpdir[];
    int port[];

    QuorumCnxManager cnxManager;

    @Before
    public void setUp() throws Exception {
        count = 3;

        peers = new HashMap<Long,QuorumServer>(count);
        tmpdir = new File[count];
        port = new int[count];
    }

    @After
    public void tearDown() throws Exception {
        cnxManager.halt();
    }

    @Test
    public void testLostMessage() throws Exception {
        LOG.info("TestLE: {}, {}", getTestName(), count);
        for(int i = 0; i < count; i++) {
            int clientport = PortAssignment.unique();
            peers.put(Long.valueOf(i),
                    new QuorumServer(i,
                            new InetSocketAddress(clientport),
                            new InetSocketAddress(PortAssignment.unique())));
            tmpdir[i] = ClientBase.createTmpDir();
            port[i] = clientport;
        }

        
        QuorumPeer peer = new QuorumPeer(peers, tmpdir[1], tmpdir[1], port[1], 3, 1, 1000, 2, 2);
        peer.startLeaderElection();
        FLETestUtils.LEThread thread = new FLETestUtils.LEThread(peer, 1);
        thread.start();

        
        mockServer();
        thread.join(5000);
        if (thread.isAlive()) {
            Assert.fail("Threads didn't join");
        }
    }

    void mockServer() throws InterruptedException, IOException {
        QuorumPeer peer = new QuorumPeer(peers, tmpdir[0], tmpdir[0], port[0], 3, 0, 1000, 2, 2);
        cnxManager = peer.createCnxnManager();
        cnxManager.listener.start();

        cnxManager.toSend(1l, FLETestUtils.createMsg(ServerState.LOOKING.ordinal(), 0, 0, 0));
        cnxManager.recvQueue.take();
        cnxManager.toSend(1L, FLETestUtils.createMsg(ServerState.FOLLOWING.ordinal(), 1, 0, 0));
    }
}
