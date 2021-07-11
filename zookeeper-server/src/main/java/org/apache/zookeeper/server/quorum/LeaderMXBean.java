package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServerMXBean;


public interface LeaderMXBean extends ZooKeeperServerMXBean {
    
    public String getCurrentZxid();

    
    public String followerInfo();

    
    public long getElectionTimeTaken();

    
    public int getLastProposalSize();

    
    public int getMinProposalSize();

    
    public int getMaxProposalSize();

    
    public void resetProposalStatistics();
}
