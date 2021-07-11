package org.apache.zookeeper.server.quorum.auth;

import java.net.Socket;


public class NullQuorumAuthLearner implements QuorumAuthLearner {

    @Override
    public void authenticate(Socket sock, String hostname) {
        return;     }
}
