package org.apache.zookeeper.server.quorum;


public interface QuorumMXBean {
    
    public String getName();
    
    
    public int getQuorumSize();

    
    public boolean isSslQuorum();

    
    public boolean isPortUnification();
}
