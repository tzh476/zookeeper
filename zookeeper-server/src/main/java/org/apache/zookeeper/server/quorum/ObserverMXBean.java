package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServerMXBean;


public interface ObserverMXBean extends ZooKeeperServerMXBean {
    
    public int getPendingRevalidationCount();
    
    
    public String getQuorumAddress();
}
