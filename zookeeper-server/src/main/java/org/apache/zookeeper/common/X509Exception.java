package org.apache.zookeeper.common;

@SuppressWarnings("serial")
public class X509Exception extends Exception {
    public X509Exception(String message) {
        super(message);
    }

    public X509Exception(Throwable cause) {
        super(cause);
    }

    public X509Exception(String message, Throwable cause) {
        super(message, cause);
    }

    public static class KeyManagerException extends X509Exception {
        public KeyManagerException(String message) {
            super(message);
        }

        public KeyManagerException(Throwable cause) {
            super(cause);
        }
    }

    public static class TrustManagerException extends X509Exception {
        public TrustManagerException(String message) {
            super(message);
        }

        public TrustManagerException(Throwable cause) {
            super(cause);
        }
    }

    public static class SSLContextException extends X509Exception {
        public SSLContextException(String message) {
            super(message);
        }

        public SSLContextException(Throwable cause) {
            super(cause);
        }

        public SSLContextException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
