package org.apache.zookeeper.common;

public class ClientX509Util extends X509Util {

    private final String sslAuthProviderProperty = getConfigPrefix() + "authProvider";

    @Override
    protected String getConfigPrefix() {
        return "zookeeper.ssl.";
    }

    @Override
    protected boolean shouldVerifyClientHostname() {
        return false;
    }

    public String getSslAuthProviderProperty() {
        return sslAuthProviderProperty;
    }
}
