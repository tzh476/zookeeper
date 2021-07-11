package org.apache.zookeeper.common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.Security;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class X509UtilTest extends BaseX509ParameterizedTestCase {
    private X509Util x509Util;
    private static final String[] customCipherSuites = new String[]{
            "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA",
            "SSL_DH_anon_EXPORT_WITH_DES40_CBC_SHA"};

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return BaseX509ParameterizedTestCase.defaultParams();
    }

    public X509UtilTest(
            X509KeyType caKeyType,
            X509KeyType certKeyType,
            String keyPassword,
            Integer paramIndex) {
        super(paramIndex, () -> {
            try {
                return X509TestContext.newBuilder()
                        .setTempDir(tempDir)
                        .setKeyStorePassword(keyPassword)
                        .setKeyStoreKeyType(certKeyType)
                        .setTrustStorePassword(keyPassword)
                        .setTrustStoreKeyType(caKeyType)
                        .build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Before
    public void setUp() throws Exception {
        try (X509Util x509util = new ClientX509Util()) {
            x509TestContext.setSystemProperties(x509util, KeyStoreFileType.JKS, KeyStoreFileType.JKS);
        }
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, "org.apache.zookeeper.ClientCnxnSocketNetty");
        x509Util = new ClientX509Util();
    }

    @After
    public void cleanUp() {
        x509TestContext.clearSystemProperties(x509Util);
        System.clearProperty(x509Util.getSslOcspEnabledProperty());
        System.clearProperty(x509Util.getSslCrlEnabledProperty());
        System.clearProperty(x509Util.getCipherSuitesProperty());
        System.clearProperty(x509Util.getSslProtocolProperty());
        System.clearProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty());
        System.clearProperty("com.sun.net.ssl.checkRevocation");
        System.clearProperty("com.sun.security.enableCRLDP");
        Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
        Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
        System.clearProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        x509Util.close();
    }

    @Test(timeout = 5000)
    public void testCreateSSLContextWithoutCustomProtocol() throws Exception {
        SSLContext sslContext = x509Util.getDefaultSSLContext();
        Assert.assertEquals(X509Util.DEFAULT_PROTOCOL, sslContext.getProtocol());
    }

    @Test(timeout = 5000)
    public void testCreateSSLContextWithCustomProtocol() throws Exception {
        final String protocol = "TLSv1.1";
        System.setProperty(x509Util.getSslProtocolProperty(), protocol);
        SSLContext sslContext = x509Util.getDefaultSSLContext();
        Assert.assertEquals(protocol, sslContext.getProtocol());
    }

    @Test(timeout = 5000)
    public void testCreateSSLContextWithoutKeyStoreLocation() throws Exception {
        System.clearProperty(x509Util.getSslKeystoreLocationProperty());
        x509Util.getDefaultSSLContext();
    }

    @Test(timeout = 5000, expected = X509Exception.SSLContextException.class)
    public void testCreateSSLContextWithoutKeyStorePassword() throws Exception {
        if (!x509TestContext.isKeyStoreEncrypted()) {
            throw new X509Exception.SSLContextException("");
        }
        System.clearProperty(x509Util.getSslKeystorePasswdProperty());
        x509Util.getDefaultSSLContext();
    }

    @Test(timeout = 5000)
    public void testCreateSSLContextWithCustomCipherSuites() throws Exception {
        setCustomCipherSuites();
        SSLSocket sslSocket = x509Util.createSSLSocket();
        Assert.assertArrayEquals(customCipherSuites, sslSocket.getEnabledCipherSuites());
    }

            @Test(timeout = 5000)
    public void testCRLEnabled() throws Exception {
        System.setProperty(x509Util.getSslCrlEnabledProperty(), "true");
        x509Util.getDefaultSSLContext();
        Assert.assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        Assert.assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        Assert.assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
    }

    @Test(timeout = 5000)
    public void testCRLDisabled() throws Exception {
        x509Util.getDefaultSSLContext();
        Assert.assertFalse(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        Assert.assertFalse(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        Assert.assertFalse(Boolean.valueOf(Security.getProperty("ocsp.enable")));
    }

    @Test(timeout = 5000)
    public void testOCSPEnabled() throws Exception {
        System.setProperty(x509Util.getSslOcspEnabledProperty(), "true");
        x509Util.getDefaultSSLContext();
        Assert.assertTrue(Boolean.valueOf(System.getProperty("com.sun.net.ssl.checkRevocation")));
        Assert.assertTrue(Boolean.valueOf(System.getProperty("com.sun.security.enableCRLDP")));
        Assert.assertTrue(Boolean.valueOf(Security.getProperty("ocsp.enable")));
    }

    @Test(timeout = 5000)
    public void testCreateSSLSocket() throws Exception {
        setCustomCipherSuites();
        SSLSocket sslSocket = x509Util.createSSLSocket();
        Assert.assertArrayEquals(customCipherSuites, sslSocket.getEnabledCipherSuites());
    }

    @Test(timeout = 5000)
    public void testCreateSSLServerSocketWithoutPort() throws Exception {
        setCustomCipherSuites();
        SSLServerSocket sslServerSocket = x509Util.createSSLServerSocket();
        Assert.assertArrayEquals(customCipherSuites, sslServerSocket.getEnabledCipherSuites());
        Assert.assertTrue(sslServerSocket.getNeedClientAuth());
    }

    @Test(timeout = 5000)
    public void testCreateSSLServerSocketWithPort() throws Exception {
        setCustomCipherSuites();
        int port = PortAssignment.unique();
        SSLServerSocket sslServerSocket = x509Util.createSSLServerSocket(port);
        Assert.assertEquals(sslServerSocket.getLocalPort(), port);
        Assert.assertArrayEquals(customCipherSuites, sslServerSocket.getEnabledCipherSuites());
        Assert.assertTrue(sslServerSocket.getNeedClientAuth());
    }

    @Test
    public void testLoadPEMKeyStore() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                x509TestContext.getKeyStorePassword(),
                KeyStoreFileType.PEM.getPropertyValue());
    }

    @Test
    public void testLoadPEMKeyStoreNullPassword() throws Exception {
        if (!x509TestContext.getKeyStorePassword().isEmpty()) {
            return;
        }
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                null,
                KeyStoreFileType.PEM.getPropertyValue());
    }

    @Test
    public void testLoadPEMKeyStoreAutodetectStoreFileType() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                x509TestContext.getKeyStorePassword(),
                null );
    }

    @Test(expected = X509Exception.KeyManagerException.class)
    public void testLoadPEMKeyStoreWithWrongPassword() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                "wrong password",                 KeyStoreFileType.PEM.getPropertyValue());
    }

    @Test
    public void testLoadPEMTrustStore() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                x509TestContext.getTrustStorePassword(),
                KeyStoreFileType.PEM.getPropertyValue(),
                false,
                false,
                true,
                true);
    }

    @Test
    public void testLoadPEMTrustStoreNullPassword() throws Exception {
        if (!x509TestContext.getTrustStorePassword().isEmpty()) {
            return;
        }
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                null,
                KeyStoreFileType.PEM.getPropertyValue(),
                false,
                false,
                true,
                true);

    }

    @Test
    public void testLoadPEMTrustStoreAutodetectStoreFileType() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath(),
                x509TestContext.getTrustStorePassword(),
                null,                  false,
                false,
                true,
                true);
    }

    @Test
    public void testLoadJKSKeyStore() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getKeyStorePassword(),
                KeyStoreFileType.JKS.getPropertyValue());
    }

    @Test
    public void testLoadJKSKeyStoreNullPassword() throws Exception {
        if (!x509TestContext.getKeyStorePassword().isEmpty()) {
            return;
        }
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                null,
                KeyStoreFileType.JKS.getPropertyValue());
    }

    @Test
    public void testLoadJKSKeyStoreAutodetectStoreFileType() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getKeyStorePassword(),
                null );
    }

    @Test(expected = X509Exception.KeyManagerException.class)
    public void testLoadJKSKeyStoreWithWrongPassword() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                "wrong password",
                KeyStoreFileType.JKS.getPropertyValue());
    }

    @Test
    public void testLoadJKSTrustStore() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getTrustStorePassword(),
                KeyStoreFileType.JKS.getPropertyValue(),
                true,
                true,
                true,
                true);
    }

    @Test
    public void testLoadJKSTrustStoreNullPassword() throws Exception {
        if (!x509TestContext.getTrustStorePassword().isEmpty()) {
            return;
        }
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                null,
                KeyStoreFileType.JKS.getPropertyValue(),
                false,
                false,
                true,
                true);
    }

    @Test
    public void testLoadJKSTrustStoreAutodetectStoreFileType() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                x509TestContext.getTrustStorePassword(),
                null,                  true,
                true,
                true,
                true);
    }

    @Test(expected = X509Exception.TrustManagerException.class)
    public void testLoadJKSTrustStoreWithWrongPassword() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath(),
                "wrong password",
                KeyStoreFileType.JKS.getPropertyValue(),
                true,
                true,
                true,
                true);
    }

    @Test
    public void testLoadPKCS12KeyStore() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
            x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            x509TestContext.getKeyStorePassword(),
            KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testLoadPKCS12KeyStoreNullPassword() throws Exception {
        if (!x509TestContext.getKeyStorePassword().isEmpty()) {
            return;
        }
                X509KeyManager km = X509Util.createKeyManager(
            x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            null,
            KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testLoadPKCS12KeyStoreAutodetectStoreFileType() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
            x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            x509TestContext.getKeyStorePassword(),
            null );
    }

    @Test(expected = X509Exception.KeyManagerException.class)
    public void testLoadPKCS12KeyStoreWithWrongPassword() throws Exception {
                X509KeyManager km = X509Util.createKeyManager(
            x509TestContext.getKeyStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            "wrong password",
            KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testLoadPKCS12TrustStore() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
            x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            x509TestContext.getTrustStorePassword(),
            KeyStoreFileType.PKCS12.getPropertyValue(),
            true,
            true,
            true,
            true);
    }

    @Test
    public void testLoadPKCS12TrustStoreNullPassword() throws Exception {
        if (!x509TestContext.getTrustStorePassword().isEmpty()) {
            return;
        }
                X509TrustManager tm = X509Util.createTrustManager(
            x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            null,
            KeyStoreFileType.PKCS12.getPropertyValue(),
            false,
            false,
            true,
            true);
    }

    @Test
    public void testLoadPKCS12TrustStoreAutodetectStoreFileType() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
            x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            x509TestContext.getTrustStorePassword(),
            null,              true,
            true,
            true,
            true);
    }

    @Test(expected = X509Exception.TrustManagerException.class)
    public void testLoadPKCS12TrustStoreWithWrongPassword() throws Exception {
                X509TrustManager tm = X509Util.createTrustManager(
            x509TestContext.getTrustStoreFile(KeyStoreFileType.PKCS12).getAbsolutePath(),
            "wrong password",
            KeyStoreFileType.PKCS12.getPropertyValue(),
            true,
            true,
            true,
            true);
    }

    @Test
    public void testGetSslHandshakeDetectionTimeoutMillisProperty() {
        Assert.assertEquals(
                X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS,
                x509Util.getSslHandshakeTimeoutMillis());
                String newPropertyString = Integer.toString(X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS + 1);
        System.setProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty(), newPropertyString);
        try (X509Util tempX509Util = new ClientX509Util()) {
            Assert.assertEquals(
                    X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS + 1,
                    tempX509Util.getSslHandshakeTimeoutMillis());
        }
                System.setProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty(), "0");
        try (X509Util tempX509Util = new ClientX509Util()) {
            Assert.assertEquals(
                    X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS,
                    tempX509Util.getSslHandshakeTimeoutMillis());
        }
                System.setProperty(x509Util.getSslHandshakeDetectionTimeoutMillisProperty(), "-1");
        try (X509Util tempX509Util = new ClientX509Util()) {
            Assert.assertEquals(
                    X509Util.DEFAULT_HANDSHAKE_DETECTION_TIMEOUT_MILLIS,
                    tempX509Util.getSslHandshakeTimeoutMillis());
        }
    }

    private static void forceClose(Socket s) {
        if (s == null || s.isClosed()) {
            return;
        }
        try {
            s.close();
        } catch (IOException e) {
        }
    }

    private static void forceClose(ServerSocket s) {
        if (s == null || s.isClosed()) {
            return;
        }
        try {
            s.close();
        } catch (IOException e) {
        }
    }

            @Test(expected = SSLHandshakeException.class)
    public void testClientRenegotiationFails() throws Throwable {
        int port = PortAssignment.unique();
        ExecutorService workerPool = Executors.newCachedThreadPool();
        final SSLServerSocket listeningSocket = x509Util.createSSLServerSocket();
        SSLSocket clientSocket = null;
        SSLSocket serverSocket = null;
        final AtomicInteger handshakesCompleted = new AtomicInteger(0);
        final CountDownLatch handshakeCompleted = new CountDownLatch(1);
        try {
            InetSocketAddress localServerAddress = new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), port);
            listeningSocket.bind(localServerAddress);
            Future<SSLSocket> acceptFuture;
            acceptFuture = workerPool.submit(new Callable<SSLSocket>() {
                @Override
                public SSLSocket call() throws Exception {
                    SSLSocket sslSocket = (SSLSocket) listeningSocket.accept();
                    sslSocket.addHandshakeCompletedListener(new HandshakeCompletedListener() {
                        @Override
                        public void handshakeCompleted(HandshakeCompletedEvent handshakeCompletedEvent) {
                            handshakesCompleted.getAndIncrement();
                            handshakeCompleted.countDown();
                        }
                    });
                    Assert.assertEquals(1, sslSocket.getInputStream().read());
                    try {
                                                sslSocket.getInputStream().read();
                        return sslSocket;
                    } catch (Exception e) {
                        forceClose(sslSocket);
                        throw e;
                    }
                }
            });
            clientSocket = x509Util.createSSLSocket();
            clientSocket.connect(localServerAddress);
            clientSocket.getOutputStream().write(1);
                        clientSocket.startHandshake();
            clientSocket.getOutputStream().write(1);
                        try {
                serverSocket = acceptFuture.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        } finally {
            forceClose(serverSocket);
            forceClose(clientSocket);
            forceClose(listeningSocket);
            workerPool.shutdown();
                                    handshakeCompleted.await(5, TimeUnit.SECONDS);
            Assert.assertEquals(1, handshakesCompleted.get());
        }
    }

    @Test
    public void testGetDefaultCipherSuitesJava8() {
        String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("1.8");
                Assert.assertTrue(cipherSuites[0].contains("CBC"));
    }

    @Test
    public void testGetDefaultCipherSuitesJava9() {
        String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("9");
                Assert.assertTrue(cipherSuites[0].contains("GCM"));
    }

    @Test
    public void testGetDefaultCipherSuitesJava10() {
        String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("10");
                Assert.assertTrue(cipherSuites[0].contains("GCM"));
    }

    @Test
    public void testGetDefaultCipherSuitesJava11() {
        String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("11");
                Assert.assertTrue(cipherSuites[0].contains("GCM"));
    }

    @Test
    public void testGetDefaultCipherSuitesUnknownVersion() {
        String[] cipherSuites = X509Util.getDefaultCipherSuitesForJavaVersion("notaversion");
                Assert.assertTrue(cipherSuites[0].contains("CBC"));
    }

    @Test(expected = NullPointerException.class)
    public void testGetDefaultCipherSuitesNullVersion() {
        X509Util.getDefaultCipherSuitesForJavaVersion(null);
    }

        private void setCustomCipherSuites() {
        System.setProperty(x509Util.getCipherSuitesProperty(), customCipherSuites[0] + "," + customCipherSuites[1]);
        x509Util.close();         x509Util = new ClientX509Util();
    }
}
