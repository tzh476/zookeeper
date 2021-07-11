package org.apache.zookeeper.server.quorum.auth;

import java.io.IOException;
import java.net.Socket;


public interface QuorumAuthLearner {

    
    public void authenticate(Socket sock, String hostname) throws IOException;
}
