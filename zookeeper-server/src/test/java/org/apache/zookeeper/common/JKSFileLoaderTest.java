package org.apache.zookeeper.common;

import java.io.IOException;
import java.security.KeyStore;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JKSFileLoaderTest extends BaseX509ParameterizedTestCase {

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return BaseX509ParameterizedTestCase.defaultParams();
    }

    public JKSFileLoaderTest(
            final X509KeyType caKeyType,
            final X509KeyType certKeyType,
            final String keyPassword,
            final Integer paramIndex) {
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
    public void testLoadKeyStore() throws Exception {
        String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        KeyStore ks = new JKSFileLoader.Builder()
                .setKeyStorePath(path)
                .setKeyStorePassword(x509TestContext.getKeyStorePassword())
                .build()
                .loadKeyStore();
        Assert.assertEquals(1, ks.size());
    }

    @Test(expected = Exception.class)
    public void testLoadKeyStoreWithWrongPassword() throws Exception {
        String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setKeyStorePath(path)
                .setKeyStorePassword("wrong password")
                .build()
                .loadKeyStore();
    }

    @Test(expected = IOException.class)
    public void testLoadKeyStoreWithWrongFilePath() throws Exception {
        String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setKeyStorePath(path + ".does_not_exist")
                .setKeyStorePassword(x509TestContext.getKeyStorePassword())
                .build()
                .loadKeyStore();
    }

    @Test(expected = NullPointerException.class)
    public void testLoadKeyStoreWithNullFilePath() throws Exception {
        new JKSFileLoader.Builder()
                .setKeyStorePassword(x509TestContext.getKeyStorePassword())
                .build()
                .loadKeyStore();
    }

    @Test(expected = IOException.class)
    public void testLoadKeyStoreWithWrongFileType() throws Exception {
                String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setKeyStorePath(path)
                .setKeyStorePassword(x509TestContext.getKeyStorePassword())
                .build()
                .loadKeyStore();
    }

    @Test
    public void testLoadTrustStore() throws Exception {
        String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        KeyStore ts = new JKSFileLoader.Builder()
                .setTrustStorePath(path)
                .setTrustStorePassword(x509TestContext.getTrustStorePassword())
                .build()
                .loadTrustStore();
        Assert.assertEquals(1, ts.size());
    }

    @Test(expected = Exception.class)
    public void testLoadTrustStoreWithWrongPassword() throws Exception {
        String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setTrustStorePath(path)
                .setTrustStorePassword("wrong password")
                .build()
                .loadTrustStore();
    }

    @Test(expected = IOException.class)
    public void testLoadTrustStoreWithWrongFilePath() throws Exception {
        String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setTrustStorePath(path + ".does_not_exist")
                .setTrustStorePassword(x509TestContext.getTrustStorePassword())
                .build()
                .loadTrustStore();
    }

    @Test(expected = NullPointerException.class)
    public void testLoadTrustStoreWithNullFilePath() throws Exception {
        new JKSFileLoader.Builder()
                .setTrustStorePassword(x509TestContext.getTrustStorePassword())
                .build()
                .loadTrustStore();
    }

    @Test(expected = IOException.class)
    public void testLoadTrustStoreWithWrongFileType() throws Exception {
                String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM)
                .getAbsolutePath();
        new JKSFileLoader.Builder()
                .setTrustStorePath(path)
                .setTrustStorePassword(x509TestContext.getTrustStorePassword())
                .build()
                .loadTrustStore();
    }
}
