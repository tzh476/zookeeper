package org.apache.zookeeper.common;


import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import org.apache.zookeeper.common.X509Exception.KeyManagerException;
import org.apache.zookeeper.common.X509Exception.SSLContextException;
import org.apache.zookeeper.common.X509Exception.TrustManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class X509Util implements Closeable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(X509Util.class);

    private static final String REJECT_CLIENT_RENEGOTIATION_PROPERTY =
            "jdk.tls.rejectClientInitiatedRenegotiation";
    static {
                                                if (System.getProperty(REJECT_CLIENT_RENEGOTIATION_PROPERTY) == null) {
            LOG.info("Setting -D {}=true to disable client-initiated TLS renegotiation",
                    REJECT_CLIENT_RENEGOTIATION_PROPERTY);
            System.setProperty(REJECT_CLIENT_RENEGOTIATION_PROPERTY, Boolean.TRUE.toString());
        }
    }

    public static final String DEFAULT_PROTOCOL = "TLSv1.2";
    private static String[] getGCMCiphers() {
        return new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        };
    }

    private static String[] getCBCCiphers() {
        return new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        };
    }

    private static String[] concatArrays(String[] left, String[] right) {
        String[] result = new String[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

        private static final String[] DEFAULT_CIPHERS_JAVA8 = concatArrays(getCBCCiphers(), getGCMCiphers());
            private static final String[] DEFAULT_CIPHERS_JAVA9 = concatArrays(getGCMCiphers(), getCBCCiphers());

    public static final int DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS = 5000;

    
    public enum ClientAuth {
        NONE(io.netty.handler.ssl.ClientAuth.NONE),
        WANT(io.netty.handler.ssl.ClientAuth.OPTIONAL),
        NEED(io.netty.handler.ssl.ClientAuth.REQUIRE);

        private final io.netty.handler.ssl.ClientAuth nettyAuth;

        ClientAuth(io.netty.handler.ssl.ClientAuth nettyAuth) {
            this.nettyAuth = nettyAuth;
        }

        
        public static ClientAuth fromPropertyValue(String prop) {
            if (prop == null || prop.length() == 0) {
                return NEED;
            }
            return ClientAuth.valueOf(prop.toUpperCase());
        }

        public io.netty.handler.ssl.ClientAuth toNettyClientAuth() {
            return nettyAuth;
        }
    }

    private String sslProtocolProperty = getConfigPrefix() + "protocol";
    private String sslEnabledProtocolsProperty = getConfigPrefix() + "enabledProtocols";
    private String cipherSuitesProperty = getConfigPrefix() + "ciphersuites";
    private String sslKeystoreLocationProperty = getConfigPrefix() + "keyStore.location";
    private String sslKeystorePasswdProperty = getConfigPrefix() + "keyStore.password";
    private String sslKeystoreTypeProperty = getConfigPrefix() + "keyStore.type";
    private String sslTruststoreLocationProperty = getConfigPrefix() + "trustStore.location";
    private String sslTruststorePasswdProperty = getConfigPrefix() + "trustStore.password";
    private String sslTruststoreTypeProperty = getConfigPrefix() + "trustStore.type";
    private String sslHostnameVerificationEnabledProperty = getConfigPrefix() + "hostnameVerification";
    private String sslCrlEnabledProperty = getConfigPrefix() + "crl";
    private String sslOcspEnabledProperty = getConfigPrefix() + "ocsp";
    private String sslClientAuthProperty = getConfigPrefix() + "clientAuth";
    private String sslHandshakeDetectionTimeoutMillisProperty = getConfigPrefix() + "handshakeDetectionTimeoutMillis";

    private ZKConfig zkConfig;
    private AtomicReference<SSLContextAndOptions> defaultSSLContextAndOptions = new AtomicReference<>(null);

    private FileChangeWatcher keyStoreFileWatcher;
    private FileChangeWatcher trustStoreFileWatcher;

    public X509Util() {
        this(null);
    }

    public X509Util(ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
        keyStoreFileWatcher = trustStoreFileWatcher = null;
    }

    protected abstract String getConfigPrefix();

    protected abstract boolean shouldVerifyClientHostname();

    public String getSslProtocolProperty() {
        return sslProtocolProperty;
    }

    public String getSslEnabledProtocolsProperty() {
        return sslEnabledProtocolsProperty;
    }

    public String getCipherSuitesProperty() {
        return cipherSuitesProperty;
    }

    public String getSslKeystoreLocationProperty() {
        return sslKeystoreLocationProperty;
    }

    public String getSslCipherSuitesProperty() {
        return cipherSuitesProperty;
    }

    public String getSslKeystorePasswdProperty() {
        return sslKeystorePasswdProperty;
    }

    public String getSslKeystoreTypeProperty() {
        return sslKeystoreTypeProperty;
    }

    public String getSslTruststoreLocationProperty() {
        return sslTruststoreLocationProperty;
    }

    public String getSslTruststorePasswdProperty() {
        return sslTruststorePasswdProperty;
    }

    public String getSslTruststoreTypeProperty() {
        return sslTruststoreTypeProperty;
    }

    public String getSslHostnameVerificationEnabledProperty() {
        return sslHostnameVerificationEnabledProperty;
    }

    public String getSslCrlEnabledProperty() {
        return sslCrlEnabledProperty;
    }

    public String getSslOcspEnabledProperty() {
        return sslOcspEnabledProperty;
    }

    public String getSslClientAuthProperty() {
        return sslClientAuthProperty;
    }

    
    public String getSslHandshakeDetectionTimeoutMillisProperty() {
        return sslHandshakeDetectionTimeoutMillisProperty;
    }

    public SSLContext getDefaultSSLContext() throws X509Exception.SSLContextException {
        return getDefaultSSLContextAndOptions().getSSLContext();
    }

    public SSLContext createSSLContext(ZKConfig config) throws SSLContextException {
        return createSSLContextAndOptions(config).getSSLContext();
    }

    public SSLContextAndOptions getDefaultSSLContextAndOptions() throws X509Exception.SSLContextException {
        SSLContextAndOptions result = defaultSSLContextAndOptions.get();
        if (result == null) {
            result = createSSLContextAndOptions();
            if (!defaultSSLContextAndOptions.compareAndSet(null, result)) {
                                result = defaultSSLContextAndOptions.get();
            }
        }
        return result;
    }

    private void resetDefaultSSLContextAndOptions() throws X509Exception.SSLContextException {
        SSLContextAndOptions newContext = createSSLContextAndOptions();
        defaultSSLContextAndOptions.set(newContext);
    }

    private SSLContextAndOptions createSSLContextAndOptions() throws SSLContextException {
        
        return createSSLContextAndOptions(zkConfig == null ? new ZKConfig() : zkConfig);
    }

    
    public int getSslHandshakeTimeoutMillis() {
        try {
            SSLContextAndOptions ctx = getDefaultSSLContextAndOptions();
            return ctx.getHandshakeDetectionTimeoutMillis();
        } catch (SSLContextException e) {
            LOG.error("Error creating SSL context and options", e);
            return DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
        } catch (Exception e) {
            LOG.error("Error parsing config property " + getSslHandshakeDetectionTimeoutMillisProperty(), e);
            return DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
        }
    }

    public SSLContextAndOptions createSSLContextAndOptions(ZKConfig config) throws SSLContextException {
        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = null;

        String keyStoreLocationProp = config.getProperty(sslKeystoreLocationProperty, "");
        String keyStorePasswordProp = config.getProperty(sslKeystorePasswdProperty, "");
        String keyStoreTypeProp = config.getProperty(sslKeystoreTypeProperty);

                        
        if (keyStoreLocationProp.isEmpty()) {
            LOG.warn(getSslKeystoreLocationProperty() + " not specified");
        } else {
            try {
                keyManagers = new KeyManager[]{
                        createKeyManager(keyStoreLocationProp, keyStorePasswordProp, keyStoreTypeProp)};
            } catch (KeyManagerException keyManagerException) {
                throw new SSLContextException("Failed to create KeyManager", keyManagerException);
            } catch (IllegalArgumentException e) {
                throw new SSLContextException("Bad value for " + sslKeystoreTypeProperty + ": " + keyStoreTypeProp, e);
            }
        }

        String trustStoreLocationProp = config.getProperty(sslTruststoreLocationProperty, "");
        String trustStorePasswordProp = config.getProperty(sslTruststorePasswdProperty, "");
        String trustStoreTypeProp = config.getProperty(sslTruststoreTypeProperty);

        boolean sslCrlEnabled = config.getBoolean(this.sslCrlEnabledProperty);
        boolean sslOcspEnabled = config.getBoolean(this.sslOcspEnabledProperty);
        boolean sslServerHostnameVerificationEnabled =
                config.getBoolean(this.getSslHostnameVerificationEnabledProperty(), true);
        boolean sslClientHostnameVerificationEnabled =
                sslServerHostnameVerificationEnabled && shouldVerifyClientHostname();

        if (trustStoreLocationProp.isEmpty()) {
            LOG.warn(getSslTruststoreLocationProperty() + " not specified");
        } else {
            try {
                trustManagers = new TrustManager[]{
                        createTrustManager(trustStoreLocationProp, trustStorePasswordProp, trustStoreTypeProp, sslCrlEnabled, sslOcspEnabled,
                                sslServerHostnameVerificationEnabled, sslClientHostnameVerificationEnabled)};
            } catch (TrustManagerException trustManagerException) {
                throw new SSLContextException("Failed to create TrustManager", trustManagerException);
            } catch (IllegalArgumentException e) {
                throw new SSLContextException("Bad value for " + sslTruststoreTypeProperty + ": " + trustStoreTypeProp, e);
            }
        }

        String protocol = config.getProperty(sslProtocolProperty, DEFAULT_PROTOCOL);
        try {
            SSLContext sslContext = SSLContext.getInstance(protocol);
            sslContext.init(keyManagers, trustManagers, null);
            return new SSLContextAndOptions(this, config, sslContext);
        } catch (NoSuchAlgorithmException | KeyManagementException sslContextInitException) {
            throw new SSLContextException(sslContextInitException);
        }
    }

    
    public static X509KeyManager createKeyManager(
            String keyStoreLocation,
            String keyStorePassword,
            String keyStoreTypeProp)
            throws KeyManagerException {
        if (keyStorePassword == null) {
            keyStorePassword = "";
        }
        try {
            KeyStoreFileType storeFileType =
                    KeyStoreFileType.fromPropertyValueOrFileName(
                            keyStoreTypeProp, keyStoreLocation);
            KeyStore ks = FileKeyStoreLoaderBuilderProvider
                    .getBuilderForKeyStoreFileType(storeFileType)
                    .setKeyStorePath(keyStoreLocation)
                    .setKeyStorePassword(keyStorePassword)
                    .build()
                    .loadKeyStore();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
            kmf.init(ks, keyStorePassword.toCharArray());

            for (KeyManager km : kmf.getKeyManagers()) {
                if (km instanceof X509KeyManager) {
                    return (X509KeyManager) km;
                }
            }
            throw new KeyManagerException("Couldn't find X509KeyManager");
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new KeyManagerException(e);
        }
    }

    
    public static X509TrustManager createTrustManager(
            String trustStoreLocation,
            String trustStorePassword,
            String trustStoreTypeProp,
            boolean crlEnabled,
            boolean ocspEnabled,
            final boolean serverHostnameVerificationEnabled,
            final boolean clientHostnameVerificationEnabled)
            throws TrustManagerException {
        if (trustStorePassword == null) {
            trustStorePassword = "";
        }
        try {
            KeyStoreFileType storeFileType =
                    KeyStoreFileType.fromPropertyValueOrFileName(
                            trustStoreTypeProp, trustStoreLocation);
            KeyStore ts = FileKeyStoreLoaderBuilderProvider
                    .getBuilderForKeyStoreFileType(storeFileType)
                    .setTrustStorePath(trustStoreLocation)
                    .setTrustStorePassword(trustStorePassword)
                    .build()
                    .loadTrustStore();
            PKIXBuilderParameters pbParams = new PKIXBuilderParameters(ts, new X509CertSelector());
            if (crlEnabled || ocspEnabled) {
                pbParams.setRevocationEnabled(true);
                System.setProperty("com.sun.net.ssl.checkRevocation", "true");
                System.setProperty("com.sun.security.enableCRLDP", "true");
                if (ocspEnabled) {
                    Security.setProperty("ocsp.enable", "true");
                }
            } else {
                pbParams.setRevocationEnabled(false);
            }

                        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
            tmf.init(new CertPathTrustManagerParameters(pbParams));

            for (final TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509ExtendedTrustManager) {
                    return new ZKTrustManager((X509ExtendedTrustManager) tm,
                            serverHostnameVerificationEnabled, clientHostnameVerificationEnabled);
                }
            }
            throw new TrustManagerException("Couldn't find X509TrustManager");
        } catch (IOException | GeneralSecurityException | IllegalArgumentException e) {
            throw new TrustManagerException(e);
        }
    }

    public SSLSocket createSSLSocket() throws X509Exception, IOException {
        return getDefaultSSLContextAndOptions().createSSLSocket();
    }

    public SSLSocket createSSLSocket(Socket socket, byte[] pushbackBytes) throws X509Exception, IOException {
        return getDefaultSSLContextAndOptions().createSSLSocket(socket, pushbackBytes);
    }

    public SSLServerSocket createSSLServerSocket() throws X509Exception, IOException {
        return getDefaultSSLContextAndOptions().createSSLServerSocket();
    }

    public SSLServerSocket createSSLServerSocket(int port) throws X509Exception, IOException {
        return getDefaultSSLContextAndOptions().createSSLServerSocket(port);
    }

    static String[] getDefaultCipherSuites() {
        return getDefaultCipherSuitesForJavaVersion(System.getProperty("java.specification.version"));
    }

    static String[] getDefaultCipherSuitesForJavaVersion(String javaVersion) {
        Objects.requireNonNull(javaVersion);
        if (javaVersion.matches("\\d+")) {
                        LOG.debug("Using Java9+ optimized cipher suites for Java version {}", javaVersion);
            return DEFAULT_CIPHERS_JAVA9;
        } else if (javaVersion.startsWith("1.")) {
                        LOG.debug("Using Java8 optimized cipher suites for Java version {}", javaVersion);
            return DEFAULT_CIPHERS_JAVA8;
        } else {
            LOG.debug("Could not parse java version {}, using Java8 optimized cipher suites",
                    javaVersion);
            return DEFAULT_CIPHERS_JAVA8;
        }
    }

    private FileChangeWatcher newFileChangeWatcher(String fileLocation) throws IOException {
        if (fileLocation == null || fileLocation.isEmpty()) {
            return null;
        }
        final Path filePath = Paths.get(fileLocation).toAbsolutePath();
        Path parentPath = filePath.getParent();
        if (parentPath == null) {
            throw new IOException(
                    "Key/trust store path does not have a parent: " + filePath);
        }
        return new FileChangeWatcher(
                parentPath,
                watchEvent -> {
                    handleWatchEvent(filePath, watchEvent);
                });
    }

    
    public void enableCertFileReloading() throws IOException {
        LOG.info("enabling cert file reloading");
        ZKConfig config = zkConfig == null ? new ZKConfig() : zkConfig;
        FileChangeWatcher newKeyStoreFileWatcher =
                newFileChangeWatcher(config.getProperty(sslKeystoreLocationProperty));
        if (newKeyStoreFileWatcher != null) {
                        if (keyStoreFileWatcher != null) {
                keyStoreFileWatcher.stop();
            }
            keyStoreFileWatcher = newKeyStoreFileWatcher;
            keyStoreFileWatcher.start();
        }
        FileChangeWatcher newTrustStoreFileWatcher =
                newFileChangeWatcher(config.getProperty(sslTruststoreLocationProperty));
        if (newTrustStoreFileWatcher != null) {
                        if (trustStoreFileWatcher != null) {
                trustStoreFileWatcher.stop();
            }
            trustStoreFileWatcher = newTrustStoreFileWatcher;
            trustStoreFileWatcher.start();
        }
    }

    
    @Override
    public void close() {
        if (keyStoreFileWatcher != null) {
            keyStoreFileWatcher.stop();
            keyStoreFileWatcher = null;
        }
        if (trustStoreFileWatcher != null) {
            trustStoreFileWatcher.stop();
            trustStoreFileWatcher = null;
        }
    }

    
    private void handleWatchEvent(Path filePath, WatchEvent<?> event) {
        boolean shouldResetContext = false;
        Path dirPath = filePath.getParent();
        if (event.kind().equals(StandardWatchEventKinds.OVERFLOW)) {
                        shouldResetContext = true;
        } else if (event.kind().equals(StandardWatchEventKinds.ENTRY_MODIFY) ||
                event.kind().equals(StandardWatchEventKinds.ENTRY_CREATE)) {
            Path eventFilePath = dirPath.resolve((Path) event.context());
            if (filePath.equals(eventFilePath)) {
                shouldResetContext = true;
            }
        }
                if (shouldResetContext) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting to reset default SSL context after receiving watch event: " +
                        event.kind() + " with context: " + event.context());
            }
            try {
                this.resetDefaultSSLContextAndOptions();
            } catch (SSLContextException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring watch event and keeping previous default SSL context. Event kind: " +
                        event.kind() + " with context: " + event.context());
            }
        }
    }
}
