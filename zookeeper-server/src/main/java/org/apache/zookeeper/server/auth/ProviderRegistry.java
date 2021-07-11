package org.apache.zookeeper.server.auth;

import java.util.Enumeration;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.ZooKeeperServer;

public class ProviderRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ProviderRegistry.class);

    private static boolean initialized = false;
    private static HashMap<String, AuthenticationProvider> authenticationProviders =
        new HashMap<String, AuthenticationProvider>();

    public static void initialize() {
        synchronized (ProviderRegistry.class) {
            if (initialized)
                return;
            IPAuthenticationProvider ipp = new IPAuthenticationProvider();
            DigestAuthenticationProvider digp = new DigestAuthenticationProvider();
            authenticationProviders.put(ipp.getScheme(), ipp);
            authenticationProviders.put(digp.getScheme(), digp);
            Enumeration<Object> en = System.getProperties().keys();
            while (en.hasMoreElements()) {
                String k = (String) en.nextElement();
                if (k.startsWith("zookeeper.authProvider.")) {
                    String className = System.getProperty(k);
                    try {
                        Class<?> c = ZooKeeperServer.class.getClassLoader()
                                .loadClass(className);
                        AuthenticationProvider ap = (AuthenticationProvider) c.getDeclaredConstructor()
                                .newInstance();
                        authenticationProviders.put(ap.getScheme(), ap);
                    } catch (Exception e) {
                        LOG.warn("Problems loading " + className,e);
                    }
                }
            }
            initialized = true;
        }
    }

    public static AuthenticationProvider getProvider(String scheme) {
        if(!initialized)
            initialize();
        return authenticationProviders.get(scheme);
    }

    public static String listProviders() {
        StringBuilder sb = new StringBuilder();
        for(String s: authenticationProviders.keySet()) {
        sb.append(s + " ");
}
        return sb.toString();
    }
}
