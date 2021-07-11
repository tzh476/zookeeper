package org.apache.zookeeper.server.quorum;



public interface LocalPeerMXBean extends ServerMXBean {
    
    
    public int getTickTime();
    
    
    public int getMaxClientCnxnsPerHost();

    
    public int getMinSessionTimeout();
    
    
    public int getMaxSessionTimeout();
    
    
    public int getInitLimit();
    
    
    public int getSyncLimit();
    
    
    public int getTick();
    
    
    public String getState();
    
    
    public String getQuorumAddress();
    
    
    public int getElectionType();

    
    public String getElectionAddress();

    
    public String getClientAddress();

    
    public String getLearnerType();

    
    public long getConfigVersion();

    
    public String getQuorumSystemInfo();

    
    public boolean isPartOfEnsemble();

    
    public boolean isLeader();
}
