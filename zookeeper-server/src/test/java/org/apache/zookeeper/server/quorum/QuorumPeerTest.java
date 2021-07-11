package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class QuorumPeerTest {

    private int electionAlg = 3;
    private int tickTime = 2000;
    private int initLimit = 3;
    private int syncLimit = 3;

    
    @Test
    public void testQuorumPeerListendOnSpecifiedClientIP() throws IOException {
        long myId = 1;
        File dataDir = ClientBase.createTmpDir();
        int clientPort = PortAssignment.unique();
        Map<Long, QuorumServer> peersView = new HashMap<Long, QuorumServer>();
        InetAddress clientIP = InetAddress.getLoopbackAddress();

        peersView.put(Long.valueOf(myId),
                new QuorumServer(myId, new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, clientPort), LearnerType.PARTICIPANT));

        
        QuorumPeer peer1 = new QuorumPeer(peersView, dataDir, dataDir, clientPort, electionAlg, myId, tickTime,
                initLimit, syncLimit);
        String hostString1 = peer1.cnxnFactory.getLocalAddress().getHostString();
        assertEquals(clientIP.getHostAddress(), hostString1);

                peer1.shutdown();

        
        peersView.clear();
        clientPort = PortAssignment.unique();
        peersView.put(Long.valueOf(myId),
                new QuorumServer(myId, new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, PortAssignment.unique()),
                        new InetSocketAddress(clientIP, clientPort), LearnerType.PARTICIPANT));
        QuorumPeer peer2 = new QuorumPeer(peersView, dataDir, dataDir, clientPort, electionAlg, myId, tickTime,
                initLimit, syncLimit);
        String hostString2 = peer2.cnxnFactory.getLocalAddress().getHostString();
        assertEquals(clientIP.getHostAddress(), hostString2);
                peer2.shutdown();
    }

    @Test
    public void testLocalPeerIsLeader() throws Exception {
        long localPeerId = 7;
        QuorumPeer peer = new QuorumPeer();
        peer.setId(localPeerId);
        Vote voteLocalPeerIsLeader = new Vote(localPeerId, 0);
        peer.setCurrentVote(voteLocalPeerIsLeader);
        assertTrue(peer.isLeader(localPeerId));
    }

    @Test
    public void testLocalPeerIsNotLeader() throws Exception {
        long localPeerId = 7;
        long otherPeerId = 17;
        QuorumPeer peer = new QuorumPeer();
        peer.setId(localPeerId);
        Vote voteLocalPeerIsNotLeader = new Vote(otherPeerId, 0);
        peer.setCurrentVote(voteLocalPeerIsNotLeader);
        assertFalse(peer.isLeader(localPeerId));
    }

    @Test
    public void testIsNotLeaderBecauseNoVote() throws Exception {
        long localPeerId = 7;
        QuorumPeer peer = new QuorumPeer();
        peer.setId(localPeerId);
        peer.setCurrentVote(null);
        assertFalse(peer.isLeader(localPeerId));
    }

}
