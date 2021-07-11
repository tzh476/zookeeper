package org.apache.zookeeper.server.quorum;

import java.nio.ByteBuffer;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.FastLeaderElection;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;

public class FLETestUtils extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(FLETestUtils.class);

    
    static class LEThread extends Thread {
        private int i;
        private QuorumPeer peer;

        LEThread(QuorumPeer peer, int i) {
            this.i = i;
            this.peer = peer;
            LOG.info("Constructor: {}", getName());

        }

        public void run() {
            try {
                Vote v = null;
                peer.setPeerState(ServerState.LOOKING);
                LOG.info("Going to call leader election: {}", i);
                v = peer.getElectionAlg().lookForLeader();

                if (v == null) {
                    Assert.fail("Thread " + i + " got a null vote");
                }

                
                peer.setCurrentVote(v);

                LOG.info("Finished election: {}, {}", i, v.getId());

                Assert.assertTrue("State is not leading.", peer.getPeerState() == ServerState.LEADING);
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.info("Joining");
        }
    }

    
    static ByteBuffer createMsg(int state, long leader, long zxid, long epoch){
        return FastLeaderElection.buildMsg(state, leader, zxid, 1, epoch);
    }

}
