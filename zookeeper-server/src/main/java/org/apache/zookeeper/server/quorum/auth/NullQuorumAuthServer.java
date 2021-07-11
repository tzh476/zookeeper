package org.apache.zookeeper.server.quorum.auth;

import java.io.DataInputStream;
import java.net.Socket;


public class NullQuorumAuthServer implements QuorumAuthServer {

    @Override
    public void authenticate(final Socket sock, final DataInputStream din) {
        return;     }
}
