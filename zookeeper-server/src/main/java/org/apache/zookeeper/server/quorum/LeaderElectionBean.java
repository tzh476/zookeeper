package org.apache.zookeeper.server.quorum;

import java.util.Date;

import org.apache.zookeeper.jmx.ZKMBeanInfo;


public class LeaderElectionBean implements LeaderElectionMXBean, ZKMBeanInfo {
    private final Date startTime = new Date();

    public String getName() {
        return "LeaderElection";
    }

    public boolean isHidden() {
        return false;
    }

    public String getStartTime() {
        return startTime.toString();
    }
}
