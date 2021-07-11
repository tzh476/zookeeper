package org.apache.zookeeper.server;

import java.util.LinkedHashMap;
import java.util.Map;


public class ZooKeeperServerConf {
    
    public static final String KEY_CLIENT_PORT = "client_port";
    
    public static final String KEY_DATA_DIR = "data_dir";
    
    public static final String KEY_DATA_LOG_DIR = "data_log_dir";
    
    public static final String KEY_TICK_TIME = "tick_time";
    
    public static final String KEY_MAX_CLIENT_CNXNS = "max_client_cnxns";
    
    public static final String KEY_MIN_SESSION_TIMEOUT = "min_session_timeout";
    
    public static final String KEY_MAX_SESSION_TIMEOUT = "max_session_timeout";
    
    public static final String KEY_SERVER_ID = "server_id";

    private final int clientPort;
    private final String dataDir;
    private final String dataLogDir;
    private final int tickTime;
    private final int maxClientCnxnsPerHost;
    private final int minSessionTimeout;
    private final int maxSessionTimeout;
    private final long serverId;

    
    ZooKeeperServerConf(int clientPort, String dataDir, String dataLogDir,
                        int tickTime, int maxClientCnxnsPerHost,
                        int minSessionTimeout, int maxSessionTimeout,
                        long serverId) {
        this.clientPort = clientPort;
        this.dataDir = dataDir;
        this.dataLogDir = dataLogDir;
        this.tickTime = tickTime;
        this.maxClientCnxnsPerHost = maxClientCnxnsPerHost;
        this.minSessionTimeout = minSessionTimeout;
        this.maxSessionTimeout = maxSessionTimeout;
        this.serverId = serverId;
    }

    
    public int getClientPort() {
        return clientPort;
    }

    
    public String getDataDir() {
        return dataDir;
    }

    
    public String getDataLogDir() {
        return dataLogDir;
    }

    
    public int getTickTime() {
        return tickTime;
    }

    
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxnsPerHost;
    }

    
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    
    public long getServerId() {
        return serverId;
    }

    
    public Map<String, Object> toMap() {
        Map<String, Object> conf = new LinkedHashMap<String, Object>();
        conf.put(KEY_CLIENT_PORT, clientPort);
        conf.put(KEY_DATA_DIR, dataDir);
        conf.put(KEY_DATA_LOG_DIR, dataLogDir);
        conf.put(KEY_TICK_TIME, tickTime);
        conf.put(KEY_MAX_CLIENT_CNXNS, maxClientCnxnsPerHost);
        conf.put(KEY_MIN_SESSION_TIMEOUT, minSessionTimeout);
        conf.put(KEY_MAX_SESSION_TIMEOUT, maxSessionTimeout);
        conf.put(KEY_SERVER_ID, serverId);
        return conf;
    }
}
