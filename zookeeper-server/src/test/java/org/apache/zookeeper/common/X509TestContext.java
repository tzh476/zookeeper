package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;

import static java.util.Objects.requireNonNull;


public class X509TestContext {
    private static final String TRUST_STORE_PREFIX = "zk_test_ca";
    private static final String KEY_STORE_PREFIX = "zk_test_key";

    private final File tempDir;

    private final X509KeyType trustStoreKeyType;
    private final KeyPair trustStoreKeyPair;
    private final long trustStoreCertExpirationMillis;
    private final X509Certificate trustStoreCertificate;
    private final String trustStorePassword;
    private File trustStoreJksFile;
    private File trustStorePemFile;
    private File trustStorePkcs12File;

    private final X509KeyType keyStoreKeyType;
    private final KeyPair keyStoreKeyPair;
    private final long keyStoreCertExpirationMillis;
    private final X509Certificate keyStoreCertificate;
    private final String keyStorePassword;
    private File keyStoreJksFile;
    private File keyStorePemFile;
    private File keyStorePkcs12File;

    private final Boolean hostnameVerification;

    
    private X509TestContext(File tempDir,
                            KeyPair trustStoreKeyPair,
                            long trustStoreCertExpirationMillis,
                            String trustStorePassword,
                            KeyPair keyStoreKeyPair,
                            long keyStoreCertExpirationMillis,
                            String keyStorePassword,
                            Boolean hostnameVerification) throws IOException, GeneralSecurityException, OperatorCreationException {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            throw new IllegalStateException("BC Security provider was not found");
        }
        this.tempDir = requireNonNull(tempDir);
        if (!tempDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + tempDir);
        }
        this.trustStoreKeyPair = requireNonNull(trustStoreKeyPair);
        this.trustStoreKeyType = keyPairToType(trustStoreKeyPair);
        this.trustStoreCertExpirationMillis = trustStoreCertExpirationMillis;
        this.trustStorePassword = requireNonNull(trustStorePassword);
        this.keyStoreKeyPair = requireNonNull(keyStoreKeyPair);
        this.keyStoreKeyType = keyPairToType(keyStoreKeyPair);
        this.keyStoreCertExpirationMillis = keyStoreCertExpirationMillis;
        this.keyStorePassword = requireNonNull(keyStorePassword);

        X500NameBuilder caNameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
        caNameBuilder.addRDN(BCStyle.CN, MethodHandles.lookup().lookupClass().getCanonicalName() + " Root CA");
        trustStoreCertificate = X509TestHelpers.newSelfSignedCACert(
                caNameBuilder.build(),
                trustStoreKeyPair,
                trustStoreCertExpirationMillis);

        X500NameBuilder nameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
        nameBuilder.addRDN(BCStyle.CN, MethodHandles.lookup().lookupClass().getCanonicalName() + " Zookeeper Test");
        keyStoreCertificate = X509TestHelpers.newCert(
                trustStoreCertificate,
                trustStoreKeyPair,
                nameBuilder.build(),
                keyStoreKeyPair.getPublic(),
                keyStoreCertExpirationMillis);
        trustStorePkcs12File = trustStorePemFile = trustStoreJksFile = null;
        keyStorePkcs12File = keyStorePemFile = keyStoreJksFile = null;

        this.hostnameVerification = hostnameVerification;
    }

    
    private X509KeyType keyPairToType(KeyPair keyPair) {
        if (keyPair.getPrivate().getAlgorithm().contains("RSA")) {
            return X509KeyType.RSA;
        } else {
            return X509KeyType.EC;
        }
    }

    public File getTempDir() {
        return tempDir;
    }

    public X509KeyType getTrustStoreKeyType() {
        return trustStoreKeyType;
    }

    public KeyPair getTrustStoreKeyPair() {
        return trustStoreKeyPair;
    }

    public long getTrustStoreCertExpirationMillis() {
        return trustStoreCertExpirationMillis;
    }

    public X509Certificate getTrustStoreCertificate() {
        return trustStoreCertificate;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    
    public File getTrustStoreFile(KeyStoreFileType storeFileType) throws IOException {
        switch (storeFileType) {
            case JKS:
                return getTrustStoreJksFile();
            case PEM:
                return getTrustStorePemFile();
            case PKCS12:
                return getTrustStorePkcs12File();
            default:
                throw new IllegalArgumentException("Invalid trust store type: " + storeFileType + ", must be one of: " +
                        Arrays.toString(KeyStoreFileType.values()));
        }
    }

    private File getTrustStoreJksFile() throws IOException {
        if (trustStoreJksFile == null) {
            File trustStoreJksFile = File.createTempFile(
                TRUST_STORE_PREFIX, KeyStoreFileType.JKS.getDefaultFileExtension(), tempDir);
            trustStoreJksFile.deleteOnExit();
            try (final FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStoreJksFile)) {
                byte[] bytes = X509TestHelpers.certToJavaTrustStoreBytes(trustStoreCertificate, trustStorePassword);
                trustStoreOutputStream.write(bytes);
                trustStoreOutputStream.flush();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            this.trustStoreJksFile = trustStoreJksFile;
        }
        return trustStoreJksFile;
    }

    private File getTrustStorePemFile() throws IOException {
        if (trustStorePemFile == null) {
            File trustStorePemFile = File.createTempFile(
                    TRUST_STORE_PREFIX, KeyStoreFileType.PEM.getDefaultFileExtension(), tempDir);
            trustStorePemFile.deleteOnExit();
            FileUtils.writeStringToFile(
                    trustStorePemFile,
                    X509TestHelpers.pemEncodeX509Certificate(trustStoreCertificate),
                    StandardCharsets.US_ASCII,
                    false);
            this.trustStorePemFile = trustStorePemFile;
        }
        return trustStorePemFile;
    }

    private File getTrustStorePkcs12File() throws IOException {
        if (trustStorePkcs12File == null) {
            File trustStorePkcs12File = File.createTempFile(
                TRUST_STORE_PREFIX, KeyStoreFileType.PKCS12.getDefaultFileExtension(), tempDir);
            trustStorePkcs12File.deleteOnExit();
            try (final FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStorePkcs12File)) {
                byte[] bytes = X509TestHelpers.certToPKCS12TrustStoreBytes(trustStoreCertificate, trustStorePassword);
                trustStoreOutputStream.write(bytes);
                trustStoreOutputStream.flush();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            this.trustStorePkcs12File = trustStorePkcs12File;
        }
        return trustStorePkcs12File;
    }

    public X509KeyType getKeyStoreKeyType() {
        return keyStoreKeyType;
    }

    public KeyPair getKeyStoreKeyPair() {
        return keyStoreKeyPair;
    }

    public long getKeyStoreCertExpirationMillis() {
        return keyStoreCertExpirationMillis;
    }

    public X509Certificate getKeyStoreCertificate() {
        return keyStoreCertificate;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public boolean isKeyStoreEncrypted() {
        return keyStorePassword.length() > 0;
    }

    
    public File getKeyStoreFile(KeyStoreFileType storeFileType) throws IOException {
        switch (storeFileType) {
            case JKS:
                return getKeyStoreJksFile();
            case PEM:
                return getKeyStorePemFile();
            case PKCS12:
                return getKeyStorePkcs12File();
            default:
                throw new IllegalArgumentException("Invalid key store type: " + storeFileType + ", must be one of: " +
                        Arrays.toString(KeyStoreFileType.values()));
        }
    }

    private File getKeyStoreJksFile() throws IOException {
        if (keyStoreJksFile == null) {
            File keyStoreJksFile = File.createTempFile(
                KEY_STORE_PREFIX, KeyStoreFileType.JKS.getDefaultFileExtension(), tempDir);
            keyStoreJksFile.deleteOnExit();
            try (final FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStoreJksFile)) {
                byte[] bytes = X509TestHelpers.certAndPrivateKeyToJavaKeyStoreBytes(
                    keyStoreCertificate, keyStoreKeyPair.getPrivate(), keyStorePassword);
                keyStoreOutputStream.write(bytes);
                keyStoreOutputStream.flush();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            this.keyStoreJksFile = keyStoreJksFile;
        }
        return keyStoreJksFile;
    }

    private File getKeyStorePemFile() throws IOException {
        if (keyStorePemFile == null) {
            try {
                File keyStorePemFile = File.createTempFile(
                        KEY_STORE_PREFIX, KeyStoreFileType.PEM.getDefaultFileExtension(), tempDir);
                keyStorePemFile.deleteOnExit();
                FileUtils.writeStringToFile(
                        keyStorePemFile,
                        X509TestHelpers.pemEncodeCertAndPrivateKey(
                                keyStoreCertificate, keyStoreKeyPair.getPrivate(), keyStorePassword),
                        StandardCharsets.US_ASCII,
                        false);
                this.keyStorePemFile = keyStorePemFile;
            } catch (OperatorCreationException e) {
                throw new IOException(e);
            }
        }
        return keyStorePemFile;
    }

    private File getKeyStorePkcs12File() throws IOException {
        if (keyStorePkcs12File == null) {
            File keyStorePkcs12File = File.createTempFile(
                KEY_STORE_PREFIX, KeyStoreFileType.PKCS12.getDefaultFileExtension(), tempDir);
            keyStorePkcs12File.deleteOnExit();
            try (final FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStorePkcs12File)) {
                byte[] bytes = X509TestHelpers.certAndPrivateKeyToPKCS12Bytes(
                    keyStoreCertificate, keyStoreKeyPair.getPrivate(), keyStorePassword);
                keyStoreOutputStream.write(bytes);
                keyStoreOutputStream.flush();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
            this.keyStorePkcs12File = keyStorePkcs12File;
        }
        return keyStorePkcs12File;
    }

    
    public void setSystemProperties(X509Util x509Util,
                                    KeyStoreFileType keyStoreFileType,
                                    KeyStoreFileType trustStoreFileType) throws IOException {
        System.setProperty(
                x509Util.getSslKeystoreLocationProperty(),
                this.getKeyStoreFile(keyStoreFileType).getAbsolutePath());
        System.setProperty(x509Util.getSslKeystorePasswdProperty(), this.getKeyStorePassword());
        System.setProperty(x509Util.getSslKeystoreTypeProperty(), keyStoreFileType.getPropertyValue());
        System.setProperty(
                x509Util.getSslTruststoreLocationProperty(),
                this.getTrustStoreFile(trustStoreFileType).getAbsolutePath());
        System.setProperty(x509Util.getSslTruststorePasswdProperty(), this.getTrustStorePassword());
        System.setProperty(x509Util.getSslTruststoreTypeProperty(), trustStoreFileType.getPropertyValue());
        if (hostnameVerification != null) {
            System.setProperty(x509Util.getSslHostnameVerificationEnabledProperty(), hostnameVerification.toString());
        } else {
            System.clearProperty(x509Util.getSslHostnameVerificationEnabledProperty());
        }
    }

    
    public void clearSystemProperties(X509Util x509Util) {
        System.clearProperty(x509Util.getSslKeystoreLocationProperty());
        System.clearProperty(x509Util.getSslKeystorePasswdProperty());
        System.clearProperty(x509Util.getSslKeystoreTypeProperty());
        System.clearProperty(x509Util.getSslTruststoreLocationProperty());
        System.clearProperty(x509Util.getSslTruststorePasswdProperty());
        System.clearProperty(x509Util.getSslTruststoreTypeProperty());
        System.clearProperty(x509Util.getSslHostnameVerificationEnabledProperty());
    }

    
    public static class Builder {
        public static final long DEFAULT_CERT_EXPIRATION_MILLIS = 1000L * 60 * 60 * 24;         private File tempDir;
        private X509KeyType trustStoreKeyType;
        private String trustStorePassword;
        private long trustStoreCertExpirationMillis;
        private X509KeyType keyStoreKeyType;
        private String keyStorePassword;
        private long keyStoreCertExpirationMillis;
        private Boolean hostnameVerification;

        
        public Builder() {
            trustStoreKeyType = X509KeyType.EC;
            trustStorePassword = "";
            trustStoreCertExpirationMillis = DEFAULT_CERT_EXPIRATION_MILLIS;
            keyStoreKeyType = X509KeyType.EC;
            keyStorePassword = "";
            keyStoreCertExpirationMillis = DEFAULT_CERT_EXPIRATION_MILLIS;
            hostnameVerification = null;
        }

        
        public X509TestContext build() throws IOException, GeneralSecurityException, OperatorCreationException {
            KeyPair trustStoreKeyPair = X509TestHelpers.generateKeyPair(trustStoreKeyType);
            KeyPair keyStoreKeyPair = X509TestHelpers.generateKeyPair(keyStoreKeyType);
            return new X509TestContext(
                    tempDir,
                    trustStoreKeyPair,
                    trustStoreCertExpirationMillis,
                    trustStorePassword,
                    keyStoreKeyPair,
                    keyStoreCertExpirationMillis,
                    keyStorePassword,
                    hostnameVerification);
        }

        
        public Builder setTempDir(File tempDir) {
            this.tempDir = tempDir;
            return this;
        }

        
        public Builder setTrustStoreKeyType(X509KeyType keyType) {
            trustStoreKeyType = keyType;
            return this;
        }

        
        public Builder setTrustStorePassword(String password) {
            trustStorePassword = password;
            return this;
        }

        
        public Builder setTrustStoreCertExpirationMillis(long expirationMillis) {
            trustStoreCertExpirationMillis = expirationMillis;
            return this;
        }

        
        public Builder setKeyStoreKeyType(X509KeyType keyType) {
            keyStoreKeyType = keyType;
            return this;
        }

        
        public Builder setKeyStorePassword(String password) {
            keyStorePassword = password;
            return this;
        }

        
        public Builder setKeyStoreCertExpirationMillis(long expirationMillis) {
            keyStoreCertExpirationMillis = expirationMillis;
            return this;
        }

        
        public Builder setHostnameVerification(Boolean hostnameVerification) {
            this.hostnameVerification = hostnameVerification;
            return this;
        }
    }

    
    public static Builder newBuilder() {
        return new Builder();
    }
}
