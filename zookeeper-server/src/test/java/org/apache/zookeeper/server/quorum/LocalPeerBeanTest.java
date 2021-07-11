package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.Test;

public class LocalPeerBeanTest {

    
    @Test
    public void testClientAddress() throws Exception {
        QuorumPeer quorumPeer = new QuorumPeer();
        LocalPeerBean remotePeerBean = new LocalPeerBean(quorumPeer);

        
        String result = remotePeerBean.getClientAddress();
        assertNotNull(result);
        assertEquals(0, result.length());

        
        ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
        int clientPort = PortAssignment.unique();
        InetSocketAddress address = new InetSocketAddress(clientPort);
        cnxnFactory.configure(address, 5, false);
        quorumPeer.setCnxnFactory(cnxnFactory);

        result = remotePeerBean.getClientAddress();
        String ipv4 = "0.0.0.0:" + clientPort;
        String ipv6 = "[0:0:0:0:0:0:0:0]:" + clientPort;
        assertTrue(result.equals(ipv4) || result.equals(ipv6));
                cnxnFactory.shutdown();

        
        clientPort = PortAssignment.unique();
        InetAddress clientIP = InetAddress.getLoopbackAddress();
        address = new InetSocketAddress(clientIP, clientPort);
        cnxnFactory = ServerCnxnFactory.createFactory();
        cnxnFactory.configure(address, 5, false);
        quorumPeer.setCnxnFactory(cnxnFactory);

        result = remotePeerBean.getClientAddress();
        String expectedResult = clientIP.getHostAddress() + ":" + clientPort;
        assertEquals(expectedResult, result);
                cnxnFactory.shutdown();
    }

    @Test
    public void testLocalPeerIsLeader() throws Exception {
        long localPeerId = 7;
        QuorumPeer peer = mock(QuorumPeer.class);
        when(peer.getId()).thenReturn(localPeerId);
        when(peer.isLeader(eq(localPeerId))).thenReturn(true);
        LocalPeerBean localPeerBean = new LocalPeerBean(peer);
        assertTrue(localPeerBean.isLeader());
    }

    @Test
    public void testLocalPeerIsNotLeader() throws Exception {
        long localPeerId = 7;
        QuorumPeer peer = mock(QuorumPeer.class);
        when(peer.getId()).thenReturn(localPeerId);
        when(peer.isLeader(eq(localPeerId))).thenReturn(false);
        LocalPeerBean localPeerBean = new LocalPeerBean(peer);
        assertFalse(localPeerBean.isLeader());
    }

}
