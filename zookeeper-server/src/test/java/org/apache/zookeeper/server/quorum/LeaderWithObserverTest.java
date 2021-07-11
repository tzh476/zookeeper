package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import static org.apache.zookeeper.server.quorum.ZabUtils.createLeader;
import static org.apache.zookeeper.server.quorum.ZabUtils.createQuorumPeer;

public class LeaderWithObserverTest {

    QuorumPeer peer;
    Leader leader;
    File tmpDir;
    long participantId;
    long observerId;

    @Before
    public void setUp() throws Exception {
        tmpDir = ClientBase.createTmpDir();
        peer = createQuorumPeer(tmpDir);
        participantId = 1;
        Map<Long, QuorumPeer.QuorumServer> peers = peer.getQuorumVerifier().getAllMembers();
        observerId = peers.size();
        leader = createLeader(tmpDir, peer);
        peer.leader = leader;
        peers.put(observerId, new QuorumPeer.QuorumServer(
                observerId, new InetSocketAddress("127.0.0.1", PortAssignment.unique()),
                new InetSocketAddress("127.0.0.1", PortAssignment.unique()),
                new InetSocketAddress("127.0.0.1", PortAssignment.unique()),
                QuorumPeer.LearnerType.OBSERVER));

                peer.tickTime = 1;
    }

    @After
    public void tearDown(){
        leader.shutdown("end of test");
        tmpDir.delete();
    }

    @Test
    public void testGetEpochToPropose() throws Exception {
        long lastAcceptedEpoch = 5;
        peer.setAcceptedEpoch(5);

        Assert.assertEquals("Unexpected vote in connectingFollowers", 0, leader.connectingFollowers.size());
        Assert.assertTrue(leader.waitingForNewEpoch);
        try {
                                    leader.getEpochToPropose(peer.getId(), lastAcceptedEpoch);
        } catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in connectingFollowers", 1, leader.connectingFollowers.size());
        Assert.assertEquals("Leader shouldn't set new epoch until quorum of participants is in connectingFollowers",
                lastAcceptedEpoch, peer.getAcceptedEpoch());
        Assert.assertTrue(leader.waitingForNewEpoch);
        try {
                        leader.getEpochToPropose(observerId, lastAcceptedEpoch);
        } catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in connectingFollowers", 1, leader.connectingFollowers.size());
        Assert.assertEquals("Leader shouldn't set new epoch after observer asks for epoch",
                lastAcceptedEpoch, peer.getAcceptedEpoch());
        Assert.assertTrue(leader.waitingForNewEpoch);
        try {
                                    leader.getEpochToPropose(participantId, lastAcceptedEpoch);
        } catch (Exception e) {
            Assert.fail("Timed out in getEpochToPropose");
        }

        Assert.assertEquals("Unexpected vote in connectingFollowers", 2, leader.connectingFollowers.size());
        Assert.assertEquals("Leader should record next epoch", lastAcceptedEpoch + 1, peer.getAcceptedEpoch());
        Assert.assertFalse(leader.waitingForNewEpoch);
    }

    @Test
    public void testWaitForEpochAck() throws Exception {
                leader.leaderStateSummary = new StateSummary(leader.self.getCurrentEpoch(), leader.zk.getLastProcessedZxid());

        Assert.assertEquals("Unexpected vote in electingFollowers", 0, leader.electingFollowers.size());
        Assert.assertFalse(leader.electionFinished);
        try {
                        leader.waitForEpochAck(peer.getId(), new StateSummary(0, 0));
        }  catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in electingFollowers", 1, leader.electingFollowers.size());
        Assert.assertFalse(leader.electionFinished);
        try {
                        leader.waitForEpochAck(observerId, new StateSummary(0, 0));
        }  catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in electingFollowers", 1, leader.electingFollowers.size());
        Assert.assertFalse(leader.electionFinished);
        try {
                        leader.waitForEpochAck(participantId, new StateSummary(0, 0));
            Assert.assertEquals("Unexpected vote in electingFollowers", 2, leader.electingFollowers.size());
            Assert.assertTrue(leader.electionFinished);
        } catch (Exception e) {
            Assert.fail("Timed out in waitForEpochAck");
        }
    }

    @Test
    public void testWaitForNewLeaderAck() throws Exception {
        long zxid = leader.zk.getZxid();

                leader.newLeaderProposal.packet = new QuorumPacket(0, zxid, null, null);
        leader.newLeaderProposal.addQuorumVerifier(peer.getQuorumVerifier());

        Set<Long> ackSet = leader.newLeaderProposal.qvAcksetPairs.get(0).getAckset();
        Assert.assertEquals("Unexpected vote in ackSet", 0, ackSet.size());
        Assert.assertFalse(leader.quorumFormed);
        try {
                        leader.waitForNewLeaderAck(peer.getId(), zxid);
        }  catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in ackSet", 1, ackSet.size());
        Assert.assertFalse(leader.quorumFormed);
        try {
                        leader.waitForNewLeaderAck(observerId, zxid);
        }  catch (InterruptedException e) {
                    }

        Assert.assertEquals("Unexpected vote in ackSet", 1, ackSet.size());
        Assert.assertFalse(leader.quorumFormed);
        try {
                        leader.waitForNewLeaderAck(participantId, zxid);
            Assert.assertEquals("Unexpected vote in ackSet", 2, ackSet.size());
            Assert.assertTrue(leader.quorumFormed);
        } catch (Exception e) {
            Assert.fail("Timed out in waitForEpochAck");
        }
    }
}
