package org.apache.zookeeper.test;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObserverLETest extends ZKTestCase {
    final QuorumBase qb = new QuorumBase();
    final ClientTest ct = new ClientTest();

    @Before
    public void establishThreeParticipantOneObserverEnsemble() throws Exception {
        qb.setUp(true);
        ct.hostPort = qb.hostPort;
        ct.setUpAll();
        qb.s5.shutdown();
    }

    @After
    public void shutdownQuorum() throws Exception {
        ct.tearDownAll();
        qb.tearDown();
    }

    
    @Test
    public void testLEWithObserver() throws Exception {
        QuorumPeer leader = null;
        for (QuorumPeer server : Arrays.asList(qb.s1, qb.s2, qb.s3)) {
            if (server.getServerState().equals(
                    QuorumStats.Provider.FOLLOWING_STATE)) {
                server.shutdown();
                assertTrue("Waiting for server down", ClientBase
                        .waitForServerDown("127.0.0.1:"
                                + server.getClientPort(),
                                ClientBase.CONNECTION_TIMEOUT));
            } else {
                assertNull("More than one leader found", leader);
                leader = server;
            }
        }
        assertTrue("Leader is not in Looking state", ClientBase
                .waitForServerState(leader, ClientBase.CONNECTION_TIMEOUT,
                        QuorumStats.Provider.LOOKING_STATE));
    }

}
