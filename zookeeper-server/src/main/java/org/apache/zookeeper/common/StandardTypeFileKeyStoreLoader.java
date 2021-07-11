package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;


abstract class StandardTypeFileKeyStoreLoader extends FileKeyStoreLoader {
    private static final char[] EMPTY_CHAR_ARRAY = new char[0];

    StandardTypeFileKeyStoreLoader(String keyStorePath,
                                   String trustStorePath,
                                   String keyStorePassword,
                                   String trustStorePassword) {
        super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    }

    @Override
    public KeyStore loadKeyStore() throws IOException, GeneralSecurityException {
        try (InputStream inputStream = new FileInputStream(new File(keyStorePath))) {
            KeyStore ks = keyStoreInstance();
            ks.load(inputStream, passwordStringToCharArray(keyStorePassword));
            return ks;
        }
    }

    @Override
    public KeyStore loadTrustStore() throws IOException, GeneralSecurityException {
        try (InputStream inputStream = new FileInputStream(new File(trustStorePath))) {
            KeyStore ts = keyStoreInstance();
            ts.load(inputStream, passwordStringToCharArray(trustStorePassword));
            return ts;
        }
    }

    protected abstract KeyStore keyStoreInstance() throws KeyStoreException;

    private static char[] passwordStringToCharArray(String password) {
        return password == null ? EMPTY_CHAR_ARRAY : password.toCharArray();
    }
}
