package org.apache.zookeeper.server.quorum;

import java.util.Date;

import org.apache.zookeeper.jmx.ZKMBeanInfo;


public abstract class ServerBean implements ServerMXBean, ZKMBeanInfo {
    private final Date startTime=new Date();
    
    public boolean isHidden() {
        return false;
    }

    public String getStartTime() {
        return startTime.toString();
    }
}
