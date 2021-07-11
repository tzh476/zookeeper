package org.apache.zookeeper.test;
import java.util.ArrayList;

import org.apache.zookeeper.jmx.CommonNames;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumMajorityTest extends QuorumBase {
    protected static final Logger LOG = LoggerFactory.getLogger(QuorumMajorityTest.class);
    public static final long CONNECTION_TIMEOUT = ClientTest.CONNECTION_TIMEOUT;

    
    
    
    
    @Test
    public void testMajQuorums() throws Throwable {
        LOG.info("Verify QuorumPeer#electionTimeTaken jmx bean attribute");

        ArrayList<QuorumPeer> peers = getPeerList();
        for (int i = 1; i <= peers.size(); i++) {
            QuorumPeer qp = peers.get(i - 1);
            Long electionTimeTaken = -1L;
            String bean = "";
            if (qp.getPeerState() == ServerState.FOLLOWING) {
                bean = String.format(
                        "%s:name0=ReplicatedServer_id%d,name1=replica.%d,name2=Follower",
                        CommonNames.DOMAIN, i, i);
            } else if (qp.getPeerState() == ServerState.LEADING) {
                bean = String.format(
                        "%s:name0=ReplicatedServer_id%d,name1=replica.%d,name2=Leader",
                        CommonNames.DOMAIN, i, i);
            }
            electionTimeTaken = (Long) JMXEnv.ensureBeanAttribute(bean,
                    "ElectionTimeTaken");
            Assert.assertTrue("Wrong electionTimeTaken value!",
                    electionTimeTaken >= 0);
        }

              setUp(false);
        
       Proposal p = new Proposal();
       
        p.addQuorumVerifier(s1.getQuorumVerifier());
        
                p.addAck(Long.valueOf(1));
        p.addAck(Long.valueOf(2));        
        Assert.assertEquals(false, p.hasAllQuorums());
        
                p.addAck(Long.valueOf(6));  
        Assert.assertEquals(false, p.hasAllQuorums());
        
                p.addAck(Long.valueOf(3));  
        Assert.assertEquals(true, p.hasAllQuorums());
        
              setUp(true);
       
       p = new Proposal();
       p.addQuorumVerifier(s1.getQuorumVerifier());
        
               p.addAck(Long.valueOf(1));      
        Assert.assertEquals(false, p.hasAllQuorums());
        
                p.addAck(Long.valueOf(4));
        p.addAck(Long.valueOf(5));
        Assert.assertEquals(false, p.hasAllQuorums());
        
                p.addAck(Long.valueOf(6));
        Assert.assertEquals(false, p.hasAllQuorums());
        
                p.addAck(Long.valueOf(2));
        Assert.assertEquals(true, p.hasAllQuorums());
    }
}
