package org.apache.zookeeper.common;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;

import org.apache.zookeeper.util.PemReader;


class PEMFileLoader extends FileKeyStoreLoader {
    private PEMFileLoader(String keyStorePath,
                          String trustStorePath,
                          String keyStorePassword,
                          String trustStorePassword) {
        super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
    }

    @Override
    public KeyStore loadKeyStore() throws IOException, GeneralSecurityException {
        Optional<String> passwordOption;
        if (keyStorePassword == null || keyStorePassword.length() == 0) {
            passwordOption = Optional.empty();
        } else {
            passwordOption = Optional.of(keyStorePassword);
        }
        File file = new File(keyStorePath);
        return PemReader.loadKeyStore(file, file, passwordOption);
    }

    @Override
    public KeyStore loadTrustStore() throws IOException, GeneralSecurityException {
        return PemReader.loadTrustStore(new File(trustStorePath));
    }


    static class Builder extends FileKeyStoreLoader.Builder<PEMFileLoader> {
        @Override
        PEMFileLoader build() {
            return new PEMFileLoader(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword);
        }
    }
}
