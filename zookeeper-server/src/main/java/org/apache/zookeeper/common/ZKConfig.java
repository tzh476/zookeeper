package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.VerifyingFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZKConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ZKConfig.class);

    public static final String JUTE_MAXBUFFER = "jute.maxbuffer";

    
    public static final String KINIT_COMMAND = "zookeeper.kinit";
    public static final String JGSS_NATIVE = "sun.security.jgss.native";

    private final Map<String, String> properties = new HashMap<String, String>();

    
    public ZKConfig() {
        init();
    }

    

    public ZKConfig(String configPath) throws ConfigException {
        this(new File(configPath));
    }

    
    public ZKConfig(File configFile) throws ConfigException {
        this();
        addConfiguration(configFile);
    }

    private void init() {
        
        handleBackwardCompatibility();
    }

    
    protected void handleBackwardCompatibility() {
        properties.put(JUTE_MAXBUFFER, System.getProperty(JUTE_MAXBUFFER));
        properties.put(KINIT_COMMAND, System.getProperty(KINIT_COMMAND));
        properties.put(JGSS_NATIVE, System.getProperty(JGSS_NATIVE));

        try (ClientX509Util clientX509Util = new ClientX509Util()) {
            putSSLProperties(clientX509Util);
            properties.put(clientX509Util.getSslAuthProviderProperty(),
                    System.getProperty(clientX509Util.getSslAuthProviderProperty()));
        }

        try (X509Util x509Util = new QuorumX509Util()) {
            putSSLProperties(x509Util);
        }
    }
    
    private void putSSLProperties(X509Util x509Util) {
        properties.put(x509Util.getSslProtocolProperty(),
                System.getProperty(x509Util.getSslProtocolProperty()));
        properties.put(x509Util.getSslEnabledProtocolsProperty(),
                System.getProperty(x509Util.getSslEnabledProtocolsProperty()));
        properties.put(x509Util.getSslCipherSuitesProperty(),
                System.getProperty(x509Util.getSslCipherSuitesProperty()));
        properties.put(x509Util.getSslKeystoreLocationProperty(),
                System.getProperty(x509Util.getSslKeystoreLocationProperty()));
        properties.put(x509Util.getSslKeystorePasswdProperty(),
                System.getProperty(x509Util.getSslKeystorePasswdProperty()));
        properties.put(x509Util.getSslKeystoreTypeProperty(),
                System.getProperty(x509Util.getSslKeystoreTypeProperty()));
        properties.put(x509Util.getSslTruststoreLocationProperty(),
                System.getProperty(x509Util.getSslTruststoreLocationProperty()));
        properties.put(x509Util.getSslTruststorePasswdProperty(),
                System.getProperty(x509Util.getSslTruststorePasswdProperty()));
        properties.put(x509Util.getSslTruststoreTypeProperty(),
                System.getProperty(x509Util.getSslTruststoreTypeProperty()));
        properties.put(x509Util.getSslHostnameVerificationEnabledProperty(),
                System.getProperty(x509Util.getSslHostnameVerificationEnabledProperty()));
        properties.put(x509Util.getSslCrlEnabledProperty(),
                System.getProperty(x509Util.getSslCrlEnabledProperty()));
        properties.put(x509Util.getSslOcspEnabledProperty(),
                System.getProperty(x509Util.getSslOcspEnabledProperty()));
        properties.put(x509Util.getSslClientAuthProperty(),
                System.getProperty(x509Util.getSslClientAuthProperty()));
        properties.put(x509Util.getSslHandshakeDetectionTimeoutMillisProperty(),
                System.getProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty()));
    }

    
    public String getProperty(String key) {
        return properties.get(key);
    }

    
    public String getProperty(String key, String defaultValue) {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value;
    }

    
    public String getJaasConfKey() {
        return System.getProperty(Environment.JAAS_CONF_KEY);
    }

    
    public void setProperty(String key, String value) {
        if (null == key) {
            throw new IllegalArgumentException("property key is null.");
        }
        String oldValue = properties.put(key, value);
        if (LOG.isDebugEnabled()) {
            if (null != oldValue && !oldValue.equals(value)) {
                LOG.debug("key {}'s value {} is replaced with new value {}", key, oldValue, value);
            }
        }
    }

    
    public void addConfiguration(File configFile) throws ConfigException {
        LOG.info("Reading configuration from: {}", configFile.getAbsolutePath());
        try {
            configFile = (new VerifyingFileFactory.Builder(LOG).warnForRelativePath().failForNonExistingPath().build())
                    .validate(configFile);
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
            parseProperties(cfg);
        } catch (IOException | IllegalArgumentException e) {
            LOG.error("Error while configuration from: {}", configFile.getAbsolutePath(), e);
            throw new ConfigException("Error while processing " + configFile.getAbsolutePath(), e);
        }
    }

    
    public void addConfiguration(String configPath) throws ConfigException {
        addConfiguration(new File(configPath));
    }

    private void parseProperties(Properties cfg) {
        for (Entry<Object, Object> entry : cfg.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            setProperty(key, value);
        }
    }

    
    public boolean getBoolean(String key) {
        return getBoolean(key, false);
    }

    
    public boolean getBoolean(String key, boolean defaultValue) {
        String propertyValue = getProperty(key);
        if (propertyValue == null) {
            return defaultValue;
        } else {
            return Boolean.parseBoolean(propertyValue.trim());
        }
    }

    
    public int getInt(String key, int defaultValue) {
        String value = getProperty(key);
        if (value != null) {
            return Integer.decode(value.trim());
        }
        return defaultValue;
    }

}
