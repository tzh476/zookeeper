package org.apache.zookeeper.server.quorum.auth;

import java.io.File;
import java.util.UUID;

import org.apache.zookeeper.util.SecurityUtils;

public class KerberosTestUtils {
    private static String keytabFile = new File(System.getProperty("build.test.dir", "build"), UUID.randomUUID().toString())
            .getAbsolutePath();

    public static String getRealm() {
        return "EXAMPLE.COM";
    }

    public static String getLearnerPrincipal() {
        return "learner@EXAMPLE.COM";
    }

    public static String getServerPrincipal() {
        return "zkquorum/localhost@EXAMPLE.COM";
    }

    public static String getClientPrincipal() {
        return getClientUsername() + "/localhost@EXAMPLE.COM";
    }

    public static String getClientUsername() {
        return "zkclient";
    }

    public static String getHostLearnerPrincipal() {
        return "learner/_HOST@EXAMPLE.COM";
    }

    public static String getHostServerPrincipal() {
        return "zkquorum/_HOST@EXAMPLE.COM";
    }

    public static String getHostNamedLearnerPrincipal(String myHostname) {
        return "learner/" + myHostname + "@EXAMPLE.COM";
    }

    public static String getKeytabFile() {
        return keytabFile;
    }

    public static String replaceHostPattern(String principal) {
        String[] components = principal.split("[/@]");
        if (components == null || components.length < 2
                || !components[1].equals(SecurityUtils.QUORUM_HOSTNAME_PATTERN)) {
            return principal;
        } else {
            return replacePattern(components, "localhost");
        }
    }

    public static String replacePattern(String[] components, String hostname) {
        if (components.length == 3) {
            return components[0] + "/" + hostname.toLowerCase() + "@"
                    + components[2];
        } else {
            return components[0] + "/" + hostname.toLowerCase();
        }
    }
}
