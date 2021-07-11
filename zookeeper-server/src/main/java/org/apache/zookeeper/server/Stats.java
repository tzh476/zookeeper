package org.apache.zookeeper.server;

import java.util.Date;


interface Stats {
    
    Date getEstablished();

    
    long getOutstandingRequests();
    
    long getPacketsReceived();
    
    long getPacketsSent();
    
    long getMinLatency();
    
    long getAvgLatency();
    
    long getMaxLatency();
    
    String getLastOperation();
    
    long getLastCxid();
    
    long getLastZxid();
    
    long getLastResponseTime();
    
    long getLastLatency();

    
    void resetStats();
}
