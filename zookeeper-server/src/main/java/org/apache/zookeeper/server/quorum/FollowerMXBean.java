package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServerMXBean;


public interface FollowerMXBean extends ZooKeeperServerMXBean {
    
    public String getQuorumAddress();
    
    
    public String getLastQueuedZxid();
    
    
    public int getPendingRevalidationCount();

    
    public long getElectionTimeTaken();
}
