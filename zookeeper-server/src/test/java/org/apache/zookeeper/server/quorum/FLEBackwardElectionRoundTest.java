package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FLEBackwardElectionRoundTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(FLELostMessageTest.class);

    int count;
    HashMap<Long,QuorumServer> peers;
    File tmpdir[];
    int port[];

    QuorumCnxManager cnxManagers[];

    @Before
    public void setUp() throws Exception {
        count = 3;

        peers = new HashMap<Long,QuorumServer>(count);
        tmpdir = new File[count];
        port = new int[count];
        cnxManagers = new QuorumCnxManager[count - 1];
    }

    @After
    public void tearDown() throws Exception {
        for(int i = 0; i < (count - 1); i++){
            if(cnxManagers[i] != null){
                cnxManagers[i].halt();
            }
        }
    }

    

    @Test
    public void testBackwardElectionRound() throws Exception {
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

        ByteBuffer initialMsg0 = getMsg();
        ByteBuffer initialMsg1 = getMsg();

        
        QuorumPeer peer = new QuorumPeer(peers, tmpdir[0], tmpdir[0], port[0], 3, 0, 1000, 2, 2);
        peer.startLeaderElection();
        FLETestUtils.LEThread thread = new FLETestUtils.LEThread(peer, 0);
        thread.start();

        
        QuorumPeer mockPeer = new QuorumPeer(peers, tmpdir[1], tmpdir[1], port[1], 3, 1, 1000, 2, 2);
        cnxManagers[0] = mockPeer.createCnxnManager();
        cnxManagers[0].listener.start();

        cnxManagers[0].toSend(0l, initialMsg0);

        
        mockPeer = new QuorumPeer(peers, tmpdir[2], tmpdir[2], port[2], 3, 2, 1000, 2, 2);
        cnxManagers[1] = mockPeer.createCnxnManager();
        cnxManagers[1].listener.start();

        cnxManagers[1].toSend(0l, initialMsg1);

        
        thread.join(5000);
        thread = new FLETestUtils.LEThread(peer, 0);
        thread.start();

        
        cnxManagers[0].toSend(0l, initialMsg0);
        cnxManagers[1].toSend(0l, initialMsg1);

        thread.join(5000);

        if (!thread.isAlive()) {
            Assert.fail("Should not have joined");
        }

    }

    private ByteBuffer getMsg() {
        return FLETestUtils.createMsg(ServerState.FOLLOWING.ordinal(), 0, 0, 1);
    }
}
