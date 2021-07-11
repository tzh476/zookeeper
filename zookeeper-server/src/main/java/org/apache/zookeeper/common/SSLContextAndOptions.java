package org.apache.zookeeper.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


public class SSLContextAndOptions {
    private static final Logger LOG = LoggerFactory.getLogger(SSLContextAndOptions.class);

    private final X509Util x509Util;
    private final String[] enabledProtocols;
    private final String[] cipherSuites;
    private final List<String> cipherSuitesAsList;
    private final X509Util.ClientAuth clientAuth;
    private final SSLContext sslContext;
    private final int handshakeDetectionTimeoutMillis;

    
    SSLContextAndOptions(final X509Util x509Util, final ZKConfig config, final SSLContext sslContext) {
        this.x509Util = requireNonNull(x509Util);
        this.sslContext = requireNonNull(sslContext);
        this.enabledProtocols = getEnabledProtocols(requireNonNull(config), sslContext);
        String[] ciphers = getCipherSuites(config);
        this.cipherSuites = ciphers;
        this.cipherSuitesAsList = Collections.unmodifiableList(Arrays.asList(ciphers));
        this.clientAuth = getClientAuth(config);
        this.handshakeDetectionTimeoutMillis = getHandshakeDetectionTimeoutMillis(config);
    }

    public SSLContext getSSLContext() {
        return sslContext;
    }

    public SSLSocket createSSLSocket() throws IOException {
        return configureSSLSocket((SSLSocket) sslContext.getSocketFactory().createSocket(), true);
    }

    public SSLSocket createSSLSocket(Socket socket, byte[] pushbackBytes) throws IOException {
        SSLSocket sslSocket;
        if (pushbackBytes != null && pushbackBytes.length > 0) {
            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(
                    socket, new ByteArrayInputStream(pushbackBytes), true);
        } else {
            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(
                    socket, null, socket.getPort(), true);
        }
        return configureSSLSocket(sslSocket, false);
    }

    public SSLServerSocket createSSLServerSocket() throws IOException {
        SSLServerSocket sslServerSocket =
                (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket();
        return configureSSLServerSocket(sslServerSocket);
    }

    public SSLServerSocket createSSLServerSocket(int port) throws IOException {
        SSLServerSocket sslServerSocket =
                (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port);
        return configureSSLServerSocket(sslServerSocket);
    }

    public SslContext createNettyJdkSslContext(SSLContext sslContext, boolean isClientSocket) {
        return new JdkSslContext(
                sslContext,
                isClientSocket,
                cipherSuitesAsList,
                IdentityCipherSuiteFilter.INSTANCE,
                null,
                isClientSocket ? X509Util.ClientAuth.NONE.toNettyClientAuth() : clientAuth.toNettyClientAuth(),
                enabledProtocols,
                false);
    }

    public int getHandshakeDetectionTimeoutMillis() {
        return handshakeDetectionTimeoutMillis;
    }

    private SSLSocket configureSSLSocket(SSLSocket socket, boolean isClientSocket) {
        SSLParameters sslParameters = socket.getSSLParameters();
        configureSslParameters(sslParameters, isClientSocket);
        socket.setSSLParameters(sslParameters);
        socket.setUseClientMode(isClientSocket);
        return socket;
    }

    private SSLServerSocket configureSSLServerSocket(SSLServerSocket socket) {
        SSLParameters sslParameters = socket.getSSLParameters();
        configureSslParameters(sslParameters, false);
        socket.setSSLParameters(sslParameters);
        socket.setUseClientMode(false);
        return socket;
    }

    private void configureSslParameters(SSLParameters sslParameters, boolean isClientSocket) {
        if (cipherSuites != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setup cipher suites for {} socket: {}",
                        isClientSocket ? "client" : "server",
                        Arrays.toString(cipherSuites));
            }
            sslParameters.setCipherSuites(cipherSuites);
        }
        if (enabledProtocols != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setup enabled protocols for {} socket: {}",
                        isClientSocket ? "client" : "server",
                        Arrays.toString(enabledProtocols));
            }
            sslParameters.setProtocols(enabledProtocols);
        }
        if (!isClientSocket) {
            switch (clientAuth) {
                case NEED:
                    sslParameters.setNeedClientAuth(true);
                    break;
                case WANT:
                    sslParameters.setWantClientAuth(true);
                    break;
                default:
                    sslParameters.setNeedClientAuth(false);                     break;
            }
        }
    }

    private String[] getEnabledProtocols(final ZKConfig config, final SSLContext sslContext) {
        String enabledProtocolsInput = config.getProperty(x509Util.getSslEnabledProtocolsProperty());
        if (enabledProtocolsInput == null) {
            return new String[] { sslContext.getProtocol() };
        }
        return enabledProtocolsInput.split(",");
    }

    private String[] getCipherSuites(final ZKConfig config) {
        String cipherSuitesInput = config.getProperty(x509Util.getSslCipherSuitesProperty());
        if (cipherSuitesInput == null) {
            return X509Util.getDefaultCipherSuites();
        } else {
            return cipherSuitesInput.split(",");
        }
    }

    private X509Util.ClientAuth getClientAuth(final ZKConfig config) {
        return X509Util.ClientAuth.fromPropertyValue(config.getProperty(x509Util.getSslClientAuthProperty()));
    }

    private int getHandshakeDetectionTimeoutMillis(final ZKConfig config) {
        String propertyString = config.getProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty());
        int result;
        if (propertyString == null) {
            result = X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
        } else {
            result = Integer.parseInt(propertyString);
            if (result < 1) {
                                                LOG.warn("Invalid value for {}: {}, using the default value of {}",
                        x509Util.getSslHandshakeDetectionTimeoutMillisProperty(),
                        result,
                        X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS);
                result = X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS;
            }
        }
        return result;
    }
}
