package org.apache.zookeeper.common;

import java.util.Objects;

public class FileKeyStoreLoaderBuilderProvider {
    
    static FileKeyStoreLoader.Builder<? extends FileKeyStoreLoader>
    getBuilderForKeyStoreFileType(KeyStoreFileType type) {
        switch (Objects.requireNonNull(type)) {
            case JKS:
                return new JKSFileLoader.Builder();
            case PEM:
                return new PEMFileLoader.Builder();
            case PKCS12:
                return new PKCS12FileLoader.Builder();
            default:
                throw new AssertionError(
                        "Unexpected StoreFileType: " + type.name());
        }
    }

}
