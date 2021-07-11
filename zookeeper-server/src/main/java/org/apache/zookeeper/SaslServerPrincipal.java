package org.apache.zookeeper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SaslServerPrincipal {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerPrincipal.class);

    
    static String getServerPrincipal(InetSocketAddress addr, ZKClientConfig clientConfig) {
        return getServerPrincipal(new WrapperInetSocketAddress(addr), clientConfig);
    }

    
    static String getServerPrincipal(WrapperInetSocketAddress addr, ZKClientConfig clientConfig) {
        String configuredServerPrincipal = clientConfig.getProperty(ZKClientConfig.ZOOKEEPER_SERVER_PRINCIPAL);
        if (configuredServerPrincipal != null) {
                        return configuredServerPrincipal;
        }
        String principalUserName = clientConfig.getProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME,
            ZKClientConfig.ZK_SASL_CLIENT_USERNAME_DEFAULT);
        String hostName = addr.getHostName();

        boolean canonicalize = true;
        String canonicalizeText = clientConfig.getProperty(ZKClientConfig.ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME,
            ZKClientConfig.ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME_DEFAULT);
        try {
            canonicalize = Boolean.parseBoolean(canonicalizeText);
        } catch (IllegalArgumentException ea) {
            LOG.warn("Could not parse config {} \"{}\" into a boolean using default {}", ZKClientConfig
                .ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME, canonicalizeText, canonicalize);
        }

        if (canonicalize) {
            WrapperInetAddress ia = addr.getAddress();
            if (ia == null) {
                throw new IllegalArgumentException("Unable to canonicalize address " + addr + " because it's not resolvable");
            }

            String canonicalHostName = ia.getCanonicalHostName();
                        if (!canonicalHostName.equals(ia.getHostAddress())) {
                hostName = canonicalHostName;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Canonicalized address to {}", hostName);
            }
        }
        String serverPrincipal = principalUserName + "/" + hostName;
        return serverPrincipal;
    }

    
    static class WrapperInetSocketAddress {
        private final InetSocketAddress addr;

        WrapperInetSocketAddress(InetSocketAddress addr) {
            this.addr = addr;
        }

        public String getHostName() {
            return addr.getHostName();
        }

        public WrapperInetAddress getAddress() {
            InetAddress ia = addr.getAddress();
            return ia == null ? null : new WrapperInetAddress(ia);
        }

        @Override
        public String toString() {
            return addr.toString();
        }
    }

    
    static class WrapperInetAddress {
        private final InetAddress ia;

        WrapperInetAddress(InetAddress ia) {
            this.ia = ia;
        }

        public String getCanonicalHostName() {
            return ia.getCanonicalHostName();
        }

        public String getHostAddress() {
            return ia.getHostAddress();
        }

        @Override
        public String toString() {
            return ia.toString();
        }
    }
}
