package org.apache.zookeeper.common;

public class QuorumX509Util extends X509Util {

    @Override
    protected String getConfigPrefix() {
        return "zookeeper.ssl.quorum.";
    }

    @Override
    protected boolean shouldVerifyClientHostname() {
        return true;
    }
}
