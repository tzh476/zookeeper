package org.apache.zookeeper.common;

import java.util.Objects;


abstract class FileKeyStoreLoader implements KeyStoreLoader {
    final String keyStorePath;
    final String trustStorePath;
    final String keyStorePassword;
    final String trustStorePassword;

    FileKeyStoreLoader(String keyStorePath,
                       String trustStorePath,
                       String keyStorePassword,
                       String trustStorePassword) {
        this.keyStorePath = keyStorePath;
        this.trustStorePath = trustStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePassword = trustStorePassword;
    }

    
    static abstract class Builder<T extends FileKeyStoreLoader> {
        String keyStorePath;
        String trustStorePath;
        String keyStorePassword;
        String trustStorePassword;

        Builder() {}

        Builder<T> setKeyStorePath(String keyStorePath) {
            this.keyStorePath = Objects.requireNonNull(keyStorePath);
            return this;
        }

        Builder<T> setTrustStorePath(String trustStorePath) {
            this.trustStorePath = Objects.requireNonNull(trustStorePath);
            return this;
        }

        Builder<T> setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = Objects.requireNonNull(keyStorePassword);
            return this;
        }

        Builder<T> setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = Objects.requireNonNull(trustStorePassword);
            return this;
        }

        abstract T build();
    }
}
