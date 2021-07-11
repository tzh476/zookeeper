package org.apache.zookeeper.server;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

import javax.management.ObjectName;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;


public class ConnectionBean implements ConnectionMXBean, ZKMBeanInfo {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionBean.class);

    private final ServerCnxn connection;
    private final Stats stats;

    private final ZooKeeperServer zk;
    
    private final String remoteIP;
    private final long sessionId;

    public ConnectionBean(ServerCnxn connection,ZooKeeperServer zk){
        this.connection = connection;
        this.stats = connection;
        this.zk = zk;
        
        InetSocketAddress sockAddr = connection.getRemoteSocketAddress();
        if (sockAddr == null) {
            remoteIP = "Unknown";
        } else {
            InetAddress addr = sockAddr.getAddress();
            if (addr instanceof Inet6Address) {
                remoteIP = ObjectName.quote(addr.getHostAddress());
            } else {
                remoteIP = addr.getHostAddress();
            }
        }
        sessionId = connection.getSessionId();
    }
    
    public String getSessionId() {
        return "0x" + Long.toHexString(sessionId);
    }

    public String getSourceIP() {
        InetSocketAddress sockAddr = connection.getRemoteSocketAddress();
        if (sockAddr == null) {
            return null;
        }
        return sockAddr.getAddress().getHostAddress()
            + ":" + sockAddr.getPort();
    }

    public String getName() {
        return MBeanRegistry.getInstance().makeFullPath("Connections", remoteIP,
                getSessionId());
    }
    
    public boolean isHidden() {
        return false;
    }
    
    public String[] getEphemeralNodes() {
        if(zk.getZKDatabase()  !=null){
            String[] res = zk.getZKDatabase().getEphemerals(sessionId)
                .toArray(new String[0]);
            Arrays.sort(res);
            return res;
        }
        return null;
    }
    
    public String getStartedTime() {
        return stats.getEstablished().toString();
    }
    
    public void terminateSession() {
        try {
            zk.closeSession(sessionId);
        } catch (Exception e) {
            LOG.warn("Unable to closeSession() for session: 0x" 
                    + getSessionId(), e);
        }
    }
    
    public void terminateConnection() {
        connection.sendCloseSession();
    }

    public void resetCounters() {
        stats.resetStats();
    }

    @Override
    public String toString() {
        return "ConnectionBean{ClientIP=" + ObjectName.quote(getSourceIP())
            + ",SessionId=0x" + getSessionId() + "}";
    }
    
    public long getOutstandingRequests() {
        return stats.getOutstandingRequests();
    }
    
    public long getPacketsReceived() {
        return stats.getPacketsReceived();
    }
    
    public long getPacketsSent() {
        return stats.getPacketsSent();
    }
    
    public int getSessionTimeout() {
        return connection.getSessionTimeout();
    }

    public long getMinLatency() {
        return stats.getMinLatency();
    }

    public long getAvgLatency() {
        return stats.getAvgLatency();
    }

    public long getMaxLatency() {
        return stats.getMaxLatency();
    }
    
    public String getLastOperation() {
        return stats.getLastOperation();
    }

    public String getLastCxid() {
        return "0x" + Long.toHexString(stats.getLastCxid());
    }

    public String getLastZxid() {
        return "0x" + Long.toHexString(stats.getLastZxid());
    }

    public String getLastResponseTime() {
        return Time.elapsedTimeToDate(stats.getLastResponseTime()).toString();
    }

    public long getLastLatency() {
        return stats.getLastLatency();
    }
}
