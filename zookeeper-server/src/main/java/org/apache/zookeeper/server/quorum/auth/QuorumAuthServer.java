package org.apache.zookeeper.server.quorum.auth;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;


public interface QuorumAuthServer {

    
    public void authenticate(Socket sock, DataInputStream din)
            throws IOException;
}
