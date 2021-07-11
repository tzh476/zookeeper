package org.apache.zookeeper.server.auth;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxn;


public interface AuthenticationProvider {
    
    String getScheme();

    
    KeeperException.Code handleAuthentication(ServerCnxn cnxn, byte authData[]);

    
    boolean matches(String id, String aclExpr);

    
    boolean isAuthenticated();

    
    boolean isValid(String id);
}
