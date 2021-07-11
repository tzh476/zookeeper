package org.apache.zookeeper.server;


public interface DataTreeMXBean {
    
    public int getNodeCount();
    
    public String getLastZxid();
    
    public int getWatchCount();
    
    
    public long approximateDataSize();
    
    public int countEphemerals();
}
