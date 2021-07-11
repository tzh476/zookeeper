package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

import java.net.InetSocketAddress;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.junit.Test;

public class RemotePeerBeanTest {

    
    @Test
    public void testGetClientAddressShouldReturnEmptyStringWhenClientAddressIsNull() {
        InetSocketAddress peerCommunicationAddress = null;
                QuorumServer peer = new QuorumServer(1, peerCommunicationAddress);
        RemotePeerBean remotePeerBean = new RemotePeerBean(null, peer);
        String clientAddress = remotePeerBean.getClientAddress();
        assertNotNull(clientAddress);
        assertEquals(0, clientAddress.length());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsLeader() {
        long peerId = 7;
        QuorumPeer.QuorumServer quorumServerMock = mock(QuorumPeer.QuorumServer.class);
        when(quorumServerMock.getId()).thenReturn(peerId);
        QuorumPeer peerMock = mock(QuorumPeer.class);
        RemotePeerBean remotePeerBean = new RemotePeerBean(peerMock, quorumServerMock);
        when(peerMock.isLeader(eq(peerId))).thenReturn(true);
        assertTrue(remotePeerBean.isLeader());
        when(peerMock.isLeader(eq(peerId))).thenReturn(false);
        assertFalse(remotePeerBean.isLeader());
    }

}
