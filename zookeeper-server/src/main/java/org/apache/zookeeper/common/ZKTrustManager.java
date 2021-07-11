package org.apache.zookeeper.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


public class ZKTrustManager extends X509ExtendedTrustManager {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTrustManager.class);

    private X509ExtendedTrustManager x509ExtendedTrustManager;
    private boolean serverHostnameVerificationEnabled;
    private boolean clientHostnameVerificationEnabled;

    private ZKHostnameVerifier hostnameVerifier;

    
    ZKTrustManager(X509ExtendedTrustManager x509ExtendedTrustManager, boolean serverHostnameVerificationEnabled,
                   boolean clientHostnameVerificationEnabled) {
        this.x509ExtendedTrustManager = x509ExtendedTrustManager;
        this.serverHostnameVerificationEnabled = serverHostnameVerificationEnabled;
        this.clientHostnameVerificationEnabled = clientHostnameVerificationEnabled;
        hostnameVerifier = new ZKHostnameVerifier();
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return x509ExtendedTrustManager.getAcceptedIssuers();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        x509ExtendedTrustManager.checkClientTrusted(chain, authType, socket);
        if (clientHostnameVerificationEnabled) {
            performHostVerification(socket.getInetAddress(), chain[0]);
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        x509ExtendedTrustManager.checkServerTrusted(chain, authType, socket);
        if (serverHostnameVerificationEnabled) {
            performHostVerification(socket.getInetAddress(), chain[0]);
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        x509ExtendedTrustManager.checkClientTrusted(chain, authType, engine);
        if (clientHostnameVerificationEnabled) {
            try {
                performHostVerification(InetAddress.getByName(engine.getPeerHost()), chain[0]);
            } catch (UnknownHostException e) {
                throw new CertificateException("Failed to verify host", e);
            }
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        x509ExtendedTrustManager.checkServerTrusted(chain, authType, engine);
        if (serverHostnameVerificationEnabled) {
            try {
                performHostVerification(InetAddress.getByName(engine.getPeerHost()), chain[0]);
            } catch (UnknownHostException e) {
                throw new CertificateException("Failed to verify host", e);
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        x509ExtendedTrustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        x509ExtendedTrustManager.checkServerTrusted(chain, authType);
    }

    
    private void performHostVerification(InetAddress inetAddress, X509Certificate certificate)
            throws CertificateException {
        String hostAddress = "";
        String hostName = "";
        try {
            hostAddress = inetAddress.getHostAddress();
            hostnameVerifier.verify(hostAddress, certificate);
        } catch (SSLException addressVerificationException) {
            try {
                LOG.debug("Failed to verify host address: {} attempting to verify host name with reverse dns lookup",
                        hostAddress, addressVerificationException);
                hostName = inetAddress.getHostName();
                hostnameVerifier.verify(hostName, certificate);
            } catch (SSLException hostnameVerificationException) {
                LOG.error("Failed to verify host address: {}", hostAddress, addressVerificationException);
                LOG.error("Failed to verify hostname: {}", hostName, hostnameVerificationException);
                throw new CertificateException("Failed to verify both host address and host name",
                        hostnameVerificationException);
            }
        }
    }
}
