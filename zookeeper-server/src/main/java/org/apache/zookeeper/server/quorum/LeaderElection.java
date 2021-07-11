package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;


@Deprecated
public class LeaderElection implements Election  {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);
    protected static final Random epochGen = new Random();

    protected QuorumPeer self;

    public LeaderElection(QuorumPeer self) {
        this.self = self;
    }

    protected static class ElectionResult {
        public Vote vote;

        public int count;

        public Vote winner;

        public int winningCount;

        public int numValidVotes;
    }

    protected ElectionResult countVotes(HashMap<InetSocketAddress, Vote> votes, HashSet<Long> heardFrom) {
        final ElectionResult result = new ElectionResult();
                result.vote = new Vote(Long.MIN_VALUE, Long.MIN_VALUE);
        result.winner = new Vote(Long.MIN_VALUE, Long.MIN_VALUE);

                                final HashMap<InetSocketAddress, Vote> validVotes = new HashMap<InetSocketAddress, Vote>();
        final Map<Long, Long> maxZxids = new HashMap<Long,Long>();
        for (Map.Entry<InetSocketAddress, Vote> e : votes.entrySet()) {
                        final Vote v = e.getValue();
            if (heardFrom.contains(v.getId())) {
                validVotes.put(e.getKey(), v);
                Long val = maxZxids.get(v.getId());
                if (val == null || val < v.getZxid()) {
                    maxZxids.put(v.getId(), v.getZxid());
            }
                    }
                }

                        for (Map.Entry<InetSocketAddress, Vote> e : validVotes.entrySet()) {
            final Vote v = e.getValue();
            Long zxid = maxZxids.get(v.getId());
            if (v.getZxid() < zxid) {
                                                e.setValue(new Vote(v.getId(), zxid, v.getElectionEpoch(), v.getPeerEpoch(), v.getState()));
            }
        }

        result.numValidVotes = validVotes.size();

        final HashMap<Vote, Integer> countTable = new HashMap<Vote, Integer>();
                for (Vote v : validVotes.values()) {
            Integer count = countTable.get(v);
            if (count == null) {
                count = 0;
            }
            countTable.put(v, count + 1);
            if (v.getId() == result.vote.getId()) {
                result.count++;
            } else if (v.getZxid() > result.vote.getZxid()
                    || (v.getZxid() == result.vote.getZxid() && v.getId() > result.vote.getId())) {
                result.vote = v;
                result.count = 1;
            }
        }
        result.winningCount = 0;
        LOG.info("Election tally: ");
        for (Entry<Vote, Integer> entry : countTable.entrySet()) {
            if (entry.getValue() > result.winningCount) {
                result.winningCount = entry.getValue();
                result.winner = entry.getKey();
            }
            LOG.info(entry.getKey().getId() + "\t-> " + entry.getValue());
        }
        return result;
    }

    
    public void shutdown(){}
    
    
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        try {
            self.setCurrentVote(new Vote(self.getId(),
                    self.getLastLoggedZxid()));
                        byte requestBytes[] = new byte[4];
            ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
            byte responseBytes[] = new byte[28];
            ByteBuffer responseBuffer = ByteBuffer.wrap(responseBytes);
            
            DatagramSocket s = null;
            try {
                s = new DatagramSocket();
                s.setSoTimeout(200);
            } catch (SocketException e1) {
                LOG.error("Socket exception when creating socket for leader election", e1);
                System.exit(4);
            }
            DatagramPacket requestPacket = new DatagramPacket(requestBytes,
                    requestBytes.length);
            DatagramPacket responsePacket = new DatagramPacket(responseBytes,
                    responseBytes.length);
            int xid = epochGen.nextInt();
            while (self.isRunning()) {
                HashMap<InetSocketAddress, Vote> votes =
                    new HashMap<InetSocketAddress, Vote>(self.getVotingView().size());

                requestBuffer.clear();
                requestBuffer.putInt(xid);
                requestPacket.setLength(4);
                HashSet<Long> heardFrom = new HashSet<Long>();
                for (QuorumServer server : self.getVotingView().values()) {
                    LOG.info("Server address: " + server.addr);
                    try {
                        requestPacket.setSocketAddress(server.addr);
                    } catch (IllegalArgumentException e) {
                                                                                                throw new IllegalArgumentException(
                                "Unable to set socket address on packet, msg:"
                                + e.getMessage() + " with addr:" + server.addr,
                                e);
                    }

                    try {
                        s.send(requestPacket);
                        responsePacket.setLength(responseBytes.length);
                        s.receive(responsePacket);
                        if (responsePacket.getLength() != responseBytes.length) {
                            LOG.error("Got a short response: "
                                    + responsePacket.getLength());
                            continue;
                        }
                        responseBuffer.clear();
                        int recvedXid = responseBuffer.getInt();
                        if (recvedXid != xid) {
                            LOG.error("Got bad xid: expected " + xid
                                    + " got " + recvedXid);
                            continue;
                        }
                        long peerId = responseBuffer.getLong();
                        heardFrom.add(peerId);
                                                    Vote vote = new Vote(responseBuffer.getLong(),
                                responseBuffer.getLong());
                            InetSocketAddress addr =
                                (InetSocketAddress) responsePacket
                                .getSocketAddress();
                            votes.put(addr, vote);
                                            } catch (IOException e) {
                        LOG.warn("Ignoring exception while looking for leader",
                                e);
                                                                    }
                }

                ElectionResult result = countVotes(votes, heardFrom);
                                                                                if (result.numValidVotes == 0) {
                    self.setCurrentVote(new Vote(self.getId(),
                            self.getLastLoggedZxid()));
                } else {
                    if (result.winner.getId() >= 0) {
                        self.setCurrentVote(result.vote);
                                                if (result.winningCount > (self.getVotingView().size() / 2)) {
                            self.setCurrentVote(result.winner);
                            s.close();
                            Vote current = self.getCurrentVote();
                            LOG.info("Found leader: my type is: " + self.getLearnerType());
                            
                            if (self.getLearnerType() == LearnerType.OBSERVER) {
                                if (current.getId() == self.getId()) {
                                                                        LOG.error("OBSERVER elected as leader!");
                                    Thread.sleep(100);
                                }
                                else {
                                    self.setPeerState(ServerState.OBSERVING);
                                    Thread.sleep(100);
                                    return current;
                                }
                            } else {
                                self.setPeerState((current.getId() == self.getId())
                                        ? ServerState.LEADING: ServerState.FOLLOWING);
                                if (self.getPeerState() == ServerState.FOLLOWING) {
                                    Thread.sleep(100);
                                }                            
                                return current;
                            }
                        }
                    }
                }
                Thread.sleep(1000);
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }
    }
}
