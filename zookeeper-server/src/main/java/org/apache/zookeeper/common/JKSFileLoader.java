package org.apache.zookeeper.common;

import java.security.KeyStore;
import java.security.KeyStoreException;


class JKSFileLoader extends StandardTypeFileKeyStoreLoader {
    private JKSFileLoader(String keyStorePath,
                          String trustStorePath,
                          String keyStorePassword,
                          String trustStorePassword) {
        super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    }

    @Override
    protected KeyStore keyStoreInstance() throws KeyStoreException {
        return KeyStore.getInstance("JKS");
    }

    static class Builder extends FileKeyStoreLoader.Builder<JKSFileLoader> {
        @Override
        JKSFileLoader build() {
            return new JKSFileLoader(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
        }
    }
}
