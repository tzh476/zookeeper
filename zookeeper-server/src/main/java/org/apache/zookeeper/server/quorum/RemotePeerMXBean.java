package org.apache.zookeeper.server.quorum;


public interface RemotePeerMXBean {
    
    public String getName();
    
    public String getQuorumAddress();

    
    public String getElectionAddress();

    
    public String getClientAddress();

    
    public String getLearnerType();

    
    public boolean isLeader();
}
