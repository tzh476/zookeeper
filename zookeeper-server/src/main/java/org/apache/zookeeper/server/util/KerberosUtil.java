package org.apache.zookeeper.server.util;

import javax.security.auth.kerberos.KerberosPrincipal;

public class KerberosUtil {
  public static String getDefaultRealm() throws IllegalArgumentException {
    return new KerberosPrincipal("tmp", 1).getRealm();
  }
}
