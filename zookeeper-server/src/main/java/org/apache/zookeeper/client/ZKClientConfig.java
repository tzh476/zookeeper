package org.apache.zookeeper.client;

import java.io.File;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;


@InterfaceAudience.Public
public class ZKClientConfig extends ZKConfig {
    public static final String ZK_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";
    public static final String ZK_SASL_CLIENT_USERNAME_DEFAULT = "zookeeper";
    public static final String ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME =
        "zookeeper.sasl.client.canonicalize.hostname";
    public static final String ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME_DEFAULT = "true";
    @SuppressWarnings("deprecation")
    public static final String LOGIN_CONTEXT_NAME_KEY = ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY;;
    public static final String LOGIN_CONTEXT_NAME_KEY_DEFAULT = "Client";
    @SuppressWarnings("deprecation")
    public static final String ENABLE_CLIENT_SASL_KEY = ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY;
    @SuppressWarnings("deprecation")
    public static final String ENABLE_CLIENT_SASL_DEFAULT = ZooKeeperSaslClient.ENABLE_CLIENT_SASL_DEFAULT;
    public static final String ZOOKEEPER_SERVER_REALM = "zookeeper.server.realm";
    
    public static final String DISABLE_AUTO_WATCH_RESET = "zookeeper.disableAutoWatchReset";
    @SuppressWarnings("deprecation")
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;
    
    @SuppressWarnings("deprecation")
    public static final String SECURE_CLIENT = ZooKeeper.SECURE_CLIENT;
    public static final int CLIENT_MAX_PACKET_LENGTH_DEFAULT = 4096 * 1024; 
    public static final String ZOOKEEPER_REQUEST_TIMEOUT = "zookeeper.request.timeout";
    public static final String ZOOKEEPER_SERVER_PRINCIPAL = "zookeeper.server.principal";
    
    public static final long ZOOKEEPER_REQUEST_TIMEOUT_DEFAULT = 0;

    public ZKClientConfig() {
        super();
        initFromJavaSystemProperties();
    }

    public ZKClientConfig(File configFile) throws ConfigException {
        super(configFile);
    }

    public ZKClientConfig(String configPath) throws ConfigException {
        super(configPath);
    }

    
    private void initFromJavaSystemProperties() {
        setProperty(ZOOKEEPER_REQUEST_TIMEOUT,
                System.getProperty(ZOOKEEPER_REQUEST_TIMEOUT));
        setProperty(ZOOKEEPER_SERVER_PRINCIPAL,
                System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL));
    }

    @Override
    protected void handleBackwardCompatibility() {
        
        super.handleBackwardCompatibility();

        
        setProperty(ZK_SASL_CLIENT_USERNAME, System.getProperty(ZK_SASL_CLIENT_USERNAME));
        setProperty(ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME, System.getProperty(ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME));
        setProperty(LOGIN_CONTEXT_NAME_KEY, System.getProperty(LOGIN_CONTEXT_NAME_KEY));
        setProperty(ENABLE_CLIENT_SASL_KEY, System.getProperty(ENABLE_CLIENT_SASL_KEY));
        setProperty(ZOOKEEPER_SERVER_REALM, System.getProperty(ZOOKEEPER_SERVER_REALM));
        setProperty(DISABLE_AUTO_WATCH_RESET, System.getProperty(DISABLE_AUTO_WATCH_RESET));
        setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, System.getProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET));
        setProperty(SECURE_CLIENT, System.getProperty(SECURE_CLIENT));
    }

    
    public boolean isSaslClientEnabled() {
        return Boolean.valueOf(getProperty(ENABLE_CLIENT_SASL_KEY, ENABLE_CLIENT_SASL_DEFAULT));
    }

    
    public long getLong(String key, long defaultValue) {
        String value = getProperty(key);
        if (value != null) {
            return Long.parseLong(value.trim());
        }
        return defaultValue;
    }
}
