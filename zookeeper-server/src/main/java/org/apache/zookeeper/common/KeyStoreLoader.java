package org.apache.zookeeper.common;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;


interface KeyStoreLoader {
    
    KeyStore loadKeyStore() throws IOException, GeneralSecurityException;

    
    KeyStore loadTrustStore() throws IOException, GeneralSecurityException;
}
