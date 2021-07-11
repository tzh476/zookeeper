package org.apache.zookeeper.util;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.zookeeper.common.BaseX509ParameterizedTestCase;
import org.apache.zookeeper.common.KeyStoreFileType;
import org.apache.zookeeper.common.X509KeyType;
import org.apache.zookeeper.common.X509TestContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PemReaderTest extends BaseX509ParameterizedTestCase {

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return BaseX509ParameterizedTestCase.defaultParams();
    }

    public PemReaderTest(
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

    @Test
    public void testLoadPrivateKeyFromKeyStore() throws IOException, GeneralSecurityException {
        Optional<String> optPassword = x509TestContext.getKeyStorePassword().length() > 0
                ? Optional.of(x509TestContext.getKeyStorePassword())
                : Optional.empty();
        PrivateKey privateKey = PemReader.loadPrivateKey(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM), optPassword);
        Assert.assertEquals(x509TestContext.getKeyStoreKeyPair().getPrivate(), privateKey);
    }

        @Test(expected = GeneralSecurityException.class)
    public void testLoadEncryptedPrivateKeyFromKeyStoreWithoutPassword() throws GeneralSecurityException, IOException {
        if (!x509TestContext.isKeyStoreEncrypted()) {
            throw new GeneralSecurityException();         }
        PemReader.loadPrivateKey(x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM), Optional.empty());
    }

        @Test(expected = GeneralSecurityException.class)
    public void testLoadEncryptedPrivateKeyFromKeyStoreWithWrongPassword() throws GeneralSecurityException, IOException {
        if (!x509TestContext.isKeyStoreEncrypted()) {
            throw new GeneralSecurityException();         }
        PemReader.loadPrivateKey(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM),
                Optional.of("wrong password"));
    }

        @Test(expected = IOException.class)
    public void testLoadUnencryptedPrivateKeyFromKeyStoreWithWrongPassword() throws GeneralSecurityException, IOException {
        if (x509TestContext.isKeyStoreEncrypted()) {
            throw new IOException();
        }
        PemReader.loadPrivateKey(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM),
                Optional.of("wrong password"));
    }

        @Test(expected = KeyStoreException.class)
    public void testLoadPrivateKeyFromTrustStore() throws IOException, GeneralSecurityException {
        PemReader.loadPrivateKey(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM), Optional.empty());
    }

        @Test(expected = KeyStoreException.class)
    public void testLoadPrivateKeyFromTrustStoreWithPassword() throws IOException, GeneralSecurityException {
        PemReader.loadPrivateKey(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM), Optional.of("foobar"));
    }

    @Test
    public void testLoadCertificateFromKeyStore() throws IOException, GeneralSecurityException {
        List<X509Certificate> certs = PemReader.readCertificateChain(
                x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM));
        Assert.assertEquals(1, certs.size());
        Assert.assertEquals(x509TestContext.getKeyStoreCertificate(), certs.get(0));
    }

    @Test
    public void testLoadCertificateFromTrustStore() throws IOException, GeneralSecurityException {
        List<X509Certificate> certs = PemReader.readCertificateChain(
                x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM));
        Assert.assertEquals(1, certs.size());
        Assert.assertEquals(x509TestContext.getTrustStoreCertificate(), certs.get(0));
    }
}
