package org.apache.zookeeper.common;

import org.bouncycastle.asn1.DERIA5String;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.OutputEncryptor;
import org.bouncycastle.operator.bc.BcContentSignerBuilder;
import org.bouncycastle.operator.bc.BcECContentSignerBuilder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.RSAKeyGenParameterSpec;
import java.util.Date;


public class X509TestHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(X509TestHelpers.class);

    private static final SecureRandom PRNG = new SecureRandom();
    private static final int DEFAULT_RSA_KEY_SIZE_BITS = 2048;
    private static final BigInteger DEFAULT_RSA_PUB_EXPONENT = RSAKeyGenParameterSpec.F4;     private static final String DEFAULT_ELLIPTIC_CURVE_NAME = "secp256r1";
        private static final int SERIAL_NUMBER_MAX_BITS = 20 * Byte.SIZE;

    
    public static X509Certificate newSelfSignedCACert(
            X500Name subject,
            KeyPair keyPair,
            long expirationMillis) throws IOException, OperatorCreationException, GeneralSecurityException {
        Date now = new Date();
        X509v3CertificateBuilder builder = initCertBuilder(
                subject,                 now,
                new Date(now.getTime() + expirationMillis),
                subject,
                keyPair.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));         builder.addExtension(
                Extension.keyUsage,
                true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyCertSign | KeyUsage.cRLSign));
        return buildAndSignCertificate(keyPair.getPrivate(), builder);
    }

    
    public static X509Certificate newCert(
            X509Certificate caCert,
            KeyPair caKeyPair,
            X500Name certSubject,
            PublicKey certPublicKey,
            long expirationMillis) throws IOException, OperatorCreationException, GeneralSecurityException {
        if (!caKeyPair.getPublic().equals(caCert.getPublicKey())) {
            throw new IllegalArgumentException("CA private key does not match the public key in the CA cert");
        }
        Date now = new Date();
        X509v3CertificateBuilder builder = initCertBuilder(
                new X500Name(caCert.getIssuerDN().getName()),
                now,
                new Date(now.getTime() + expirationMillis),
                certSubject,
                certPublicKey);
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));         builder.addExtension(
                Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        builder.addExtension(
                Extension.extendedKeyUsage,
                true,
                new ExtendedKeyUsage(new KeyPurposeId[] { KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth }));

        builder.addExtension(
                Extension.subjectAlternativeName,
                false,
                getLocalhostSubjectAltNames());
        return buildAndSignCertificate(caKeyPair.getPrivate(), builder);
    }

    
    private static GeneralNames getLocalhostSubjectAltNames() throws UnknownHostException {
        InetAddress[] localAddresses = InetAddress.getAllByName("localhost");
        GeneralName[] generalNames = new GeneralName[localAddresses.length + 1];
        for (int i = 0; i < localAddresses.length; i++) {
            generalNames[i] = new GeneralName(GeneralName.iPAddress, new DEROctetString(localAddresses[i].getAddress()));
        }
        generalNames[generalNames.length - 1] = new GeneralName(GeneralName.dNSName, new DERIA5String("localhost"));
        return new GeneralNames(generalNames);
    }

    
    private static X509v3CertificateBuilder initCertBuilder(
            X500Name issuer,
            Date notBefore,
            Date notAfter,
            X500Name subject,
            PublicKey subjectPublicKey) {
        return new X509v3CertificateBuilder(
                issuer,
                new BigInteger(SERIAL_NUMBER_MAX_BITS, PRNG),
                notBefore,
                notAfter,
                subject,
                SubjectPublicKeyInfo.getInstance(subjectPublicKey.getEncoded()));
    }

    
    private static X509Certificate buildAndSignCertificate(
            PrivateKey privateKey,
            X509v3CertificateBuilder builder) throws IOException, OperatorCreationException, CertificateException {
        BcContentSignerBuilder signerBuilder;
        if (privateKey.getAlgorithm().contains("RSA")) {             AlgorithmIdentifier signatureAlgorithm = new DefaultSignatureAlgorithmIdentifierFinder().find(
                    "SHA256WithRSAEncryption");
            AlgorithmIdentifier digestAlgorithm = new DefaultDigestAlgorithmIdentifierFinder().find(signatureAlgorithm);
            signerBuilder = new BcRSAContentSignerBuilder(signatureAlgorithm, digestAlgorithm);
        } else {             AlgorithmIdentifier signatureAlgorithm = new DefaultSignatureAlgorithmIdentifierFinder().find(
                    "SHA256withECDSA");
            AlgorithmIdentifier digestAlgorithm = new DefaultDigestAlgorithmIdentifierFinder().find(signatureAlgorithm);
            signerBuilder = new BcECContentSignerBuilder(signatureAlgorithm, digestAlgorithm);
        }
        AsymmetricKeyParameter privateKeyParam = PrivateKeyFactory.createKey(privateKey.getEncoded());
        ContentSigner signer = signerBuilder.build(privateKeyParam);
        return toX509Cert(builder.build(signer));
    }

    
    public static KeyPair generateKeyPair(X509KeyType keyType) throws GeneralSecurityException {
        switch (keyType) {
            case RSA:
                return generateRSAKeyPair();
            case EC:
                return generateECKeyPair();
            default:
                throw new IllegalArgumentException("Invalid X509KeyType");
        }
    }

    
    public static KeyPair generateRSAKeyPair() throws GeneralSecurityException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        RSAKeyGenParameterSpec keyGenSpec = new RSAKeyGenParameterSpec(
                DEFAULT_RSA_KEY_SIZE_BITS, DEFAULT_RSA_PUB_EXPONENT);
        keyGen.initialize(keyGenSpec, PRNG);
        return keyGen.generateKeyPair();
    }

    
    public static KeyPair generateECKeyPair() throws GeneralSecurityException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
        keyGen.initialize(new ECGenParameterSpec(DEFAULT_ELLIPTIC_CURVE_NAME), PRNG);
        return keyGen.generateKeyPair();
    }

    
    public static String pemEncodeCertAndPrivateKey(
            X509Certificate cert,
            PrivateKey privateKey,
            String keyPassword) throws IOException, OperatorCreationException {
        return pemEncodeX509Certificate(cert) +
                "\n" +
                pemEncodePrivateKey(privateKey, keyPassword);
    }

    
    public static String pemEncodePrivateKey(
            PrivateKey key,
            String password) throws IOException, OperatorCreationException {
        StringWriter stringWriter = new StringWriter();
        JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter);
        OutputEncryptor encryptor = null;
        if (password != null && password.length() > 0) {
            encryptor = new JceOpenSSLPKCS8EncryptorBuilder(PKCSObjectIdentifiers.pbeWithSHAAnd3_KeyTripleDES_CBC)
                    .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                    .setRandom(PRNG)
                    .setPasssword(password.toCharArray())
                    .build();
        }
        pemWriter.writeObject(new JcaPKCS8Generator(key, encryptor));
        pemWriter.close();
        return stringWriter.toString();
    }

    
    public static String pemEncodeX509Certificate(X509Certificate cert) throws IOException {
        StringWriter stringWriter = new StringWriter();
        JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter);
        pemWriter.writeObject(cert);
        pemWriter.close();
        return stringWriter.toString();
    }

    
    public static byte[] certToJavaTrustStoreBytes(
            X509Certificate cert,
            String keyPassword) throws IOException, GeneralSecurityException {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        return certToTrustStoreBytes(cert, keyPassword, trustStore);
    }

    
    public static byte[] certToPKCS12TrustStoreBytes(
            X509Certificate cert,
            String keyPassword) throws IOException, GeneralSecurityException {
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        return certToTrustStoreBytes(cert, keyPassword, trustStore);
    }

    private static byte[] certToTrustStoreBytes(X509Certificate cert,
                                                String keyPassword,
                                                KeyStore trustStore) throws IOException, GeneralSecurityException {
        char[] keyPasswordChars = keyPassword == null ? new char[0] : keyPassword.toCharArray();
        trustStore.load(null, keyPasswordChars);
        trustStore.setCertificateEntry(cert.getSubjectDN().toString(), cert);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        trustStore.store(outputStream, keyPasswordChars);
        outputStream.flush();
        byte[] result = outputStream.toByteArray();
        outputStream.close();
        return result;
    }

    
    public static byte[] certAndPrivateKeyToJavaKeyStoreBytes(
            X509Certificate cert,
            PrivateKey privateKey,
            String keyPassword) throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        return certAndPrivateKeyToPKCS12Bytes(cert, privateKey, keyPassword, keyStore);
    }

    
    public static byte[] certAndPrivateKeyToPKCS12Bytes(
            X509Certificate cert,
            PrivateKey privateKey,
            String keyPassword) throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        return certAndPrivateKeyToPKCS12Bytes(cert, privateKey, keyPassword, keyStore);
    }

    private static byte[] certAndPrivateKeyToPKCS12Bytes(
            X509Certificate cert,
            PrivateKey privateKey,
            String keyPassword,
            KeyStore keyStore) throws IOException, GeneralSecurityException {
        char[] keyPasswordChars = keyPassword == null ? new char[0] : keyPassword.toCharArray();
        keyStore.load(null, keyPasswordChars);
        keyStore.setKeyEntry(
                "key",
                privateKey,
                keyPasswordChars,
                new Certificate[] { cert });
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        keyStore.store(outputStream, keyPasswordChars);
        outputStream.flush();
        byte[] result = outputStream.toByteArray();
        outputStream.close();
        return result;
    }

    
    public static X509Certificate toX509Cert(X509CertificateHolder certHolder) throws CertificateException {
        return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certHolder);
    }
}
