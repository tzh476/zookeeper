package org.apache.zookeeper.server;


public interface ZooKeeperServerMXBean {
    
    public String getClientPort();
    
    public String getVersion();
    
    public String getStartTime();
    
    public long getMinRequestLatency();
    
    public long getAvgRequestLatency();
    
    public long getMaxRequestLatency();
    
    public long getPacketsReceived();
    
    public long getPacketsSent();
    
    public long getFsyncThresholdExceedCount();
    
    public long getOutstandingRequests();
    
    public int getTickTime();
    
    public void setTickTime(int tickTime);

    
    public int getMaxClientCnxnsPerHost();

    
    public void setMaxClientCnxnsPerHost(int max);

    
    public int getMinSessionTimeout();
    
    public void setMinSessionTimeout(int min);

    
    public int getMaxSessionTimeout();
    
    public void setMaxSessionTimeout(int max);

    
    public void resetStatistics();
    
    public void resetLatency();
    
    public void resetMaxLatency();
    
    public void resetFsyncThresholdExceedCount();
    
    public long getNumAliveConnections();

    
    public long getDataDirSize();
    
    public long getLogDirSize();

    
    public String getSecureClientPort();
    
    public String getSecureClientAddress();

    
    public long getTxnLogElapsedSyncTime();

    
    public int getJuteMaxBufferSize();

    
    public int getLastClientResponseSize();

    
    public int getMinClientResponseSize();

    
    public int getMaxClientResponseSize();
}
