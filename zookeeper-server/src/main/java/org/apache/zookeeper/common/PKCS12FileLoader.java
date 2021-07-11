package org.apache.zookeeper.common;

import java.security.KeyStore;
import java.security.KeyStoreException;


class PKCS12FileLoader extends StandardTypeFileKeyStoreLoader {
    private PKCS12FileLoader(String keyStorePath,
                             String trustStorePath,
                             String keyStorePassword,
                             String trustStorePassword) {
        super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    }

    @Override
    protected KeyStore keyStoreInstance() throws KeyStoreException {
        return KeyStore.getInstance("PKCS12");
    }

    static class Builder extends FileKeyStoreLoader.Builder<PKCS12FileLoader> {
        @Override
        PKCS12FileLoader build() {
            return new PKCS12FileLoader(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
        }
    }
}
