package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServerBean;
import org.apache.zookeeper.server.ZooKeeperServer;


public class LeaderBean extends ZooKeeperServerBean implements LeaderMXBean {
    private final Leader leader;
    
    public LeaderBean(Leader leader, ZooKeeperServer zks) {
        super(zks);
        this.leader = leader;
    }
    
    public String getName() {
        return "Leader";
    }

    public String getCurrentZxid() {
        return "0x" + Long.toHexString(zks.getZxid());
    }
    
    public String followerInfo() {
        StringBuilder sb = new StringBuilder();
        for (LearnerHandler handler : leader.getLearners()) {
            sb.append(handler.toString()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public long getElectionTimeTaken() {
        return leader.self.getElectionTimeTaken();
    }

    @Override
    public int getLastProposalSize() {
        return leader.getProposalStats().getLastBufferSize();
    }

    @Override
    public int getMinProposalSize() {
        return leader.getProposalStats().getMinBufferSize();
    }

    @Override
    public int getMaxProposalSize() {
        return leader.getProposalStats().getMaxBufferSize();
    }

    @Override
    public void resetProposalStatistics() {
        leader.getProposalStats().reset();
    }
}
