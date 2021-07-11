package org.apache.zookeeper.server;


public interface ConnectionMXBean {
    
    public String getSourceIP();
    
    public String getSessionId();
    
    public String getStartedTime();
    
    public String[] getEphemeralNodes();
    
    public long getPacketsReceived();
    
    public long getPacketsSent();
    
    public long getOutstandingRequests();
    
    public int getSessionTimeout();
    
    
    public void terminateSession();
    
    public void terminateConnection();


    
    long getMinLatency();
    
    long getAvgLatency();
    
    long getMaxLatency();
    
    String getLastOperation();
    
    String getLastCxid();
    
    String getLastZxid();
    
    String getLastResponseTime();
    
    long getLastLatency();

    
    void resetCounters();
}
