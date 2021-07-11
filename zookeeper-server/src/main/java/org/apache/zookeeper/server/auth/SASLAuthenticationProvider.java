package org.apache.zookeeper.server.auth;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxn;

public class SASLAuthenticationProvider implements AuthenticationProvider {

    public String getScheme() {
        return "sasl";
    }

    public KeeperException.Code
        handleAuthentication(ServerCnxn cnxn, byte[] authData)
    {
                                return KeeperException.Code.AUTHFAILED;

    }

    public boolean matches(String id,String aclExpr) {
        if ((id.equals("super") || id.equals(aclExpr))) {
          return true;
        }
        String readAccessUser = System.getProperty("zookeeper.letAnySaslUserDoX");
        if ( readAccessUser != null && aclExpr.equals(readAccessUser)) {
          return true;
        }
        return false;
    }

    public boolean isAuthenticated() {
        return true;
    }

    public boolean isValid(String id) {
                                                                        try {
            new KerberosName(id);
            return true;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
   }


}
