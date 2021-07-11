package org.apache.zookeeper.server.quorum;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuorumBeanTest {
    @Test
    public void testGetNameProperty() {
        QuorumPeer qpMock = mock(QuorumPeer.class);
        when(qpMock.getId()).thenReturn(1L);
        QuorumBean qb = new QuorumBean(qpMock);

        assertThat("getName property should return Bean name in the right format", qb.getName(), equalTo("ReplicatedServer_id1"));
    }

    @Test
    public void testIsHiddenProperty() {
        QuorumPeer qpMock = mock(QuorumPeer.class);
        QuorumBean qb = new QuorumBean(qpMock);
        assertThat("isHidden should return false", qb.isHidden(), equalTo(false));
    }

    @Test
    public void testGetQuorumSizeProperty() {
        QuorumPeer qpMock = mock(QuorumPeer.class);
        QuorumBean qb = new QuorumBean(qpMock);

        when(qpMock.getQuorumSize()).thenReturn(5);
        assertThat("getQuorumSize property should return value of peet.getQuorumSize()", qb.getQuorumSize(), equalTo(5));
    }

    @Test
    public void testSslQuorumProperty() {
        QuorumPeer qpMock = mock(QuorumPeer.class);
        QuorumBean qb = new QuorumBean(qpMock);

        when(qpMock.isSslQuorum()).thenReturn(true);
        assertThat("isSslQuorum property should return value of peer.isSslQuorum()", qb.isSslQuorum(), equalTo(true));
        when(qpMock.isSslQuorum()).thenReturn(false);
        assertThat("isSslQuorum property should return value of peer.isSslQuorum()", qb.isSslQuorum(), equalTo(false));
    }

    @Test
    public void testPortUnificationProperty() {
        QuorumPeer qpMock = mock(QuorumPeer.class);
        QuorumBean qb = new QuorumBean(qpMock);

        when(qpMock.shouldUsePortUnification()).thenReturn(true);
        assertThat("isPortUnification property should return value of peer.shouldUsePortUnification()", qb.isPortUnification(), equalTo(true));
        when(qpMock.shouldUsePortUnification()).thenReturn(false);
        assertThat("isPortUnification property should return value of peer.shouldUsePortUnification()", qb.isPortUnification(), equalTo(false));
    }
}
