package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.Test;

public class ObserverHierarchicalQuorumTest extends HierarchicalQuorumTest {
    
    void startServers() throws Exception {
        startServers(true);
    }
           
    protected void shutdown(QuorumPeer qp) {
        QuorumBase.shutdown(qp);
    }

    @Test
    public void testHierarchicalQuorum() throws Throwable {
        cht.runHammer(5, 10);
    }
}
