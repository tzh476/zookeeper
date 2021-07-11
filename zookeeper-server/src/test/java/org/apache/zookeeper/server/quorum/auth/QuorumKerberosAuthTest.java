package org.apache.zookeeper.server.quorum.auth;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase.MainThread;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class QuorumKerberosAuthTest extends KerberosSecurityTestcase {
    private static File keytabFile;
    static {
        String keytabFilePath = FilenameUtils.normalize(KerberosTestUtils.getKeytabFile(), true);

                                String jaasEntries = ""
                + "QuorumServer {\n"
                + "       com.sun.security.auth.module.Krb5LoginModule required\n"
                + "       useKeyTab=true\n"
                + "       keyTab=\"" + keytabFilePath + "\"\n"
                + "       storeKey=true\n"
                + "       useTicketCache=false\n"
                + "       debug=false\n"
                + "       refreshKrb5Config=true\n"
                + "       principal=\"" + KerberosTestUtils.getServerPrincipal() + "\";\n" + "};\n"
                + "QuorumLearner {\n"
                + "       com.sun.security.auth.module.Krb5LoginModule required\n"
                + "       useKeyTab=true\n"
                + "       keyTab=\"" + keytabFilePath + "\"\n"
                + "       storeKey=true\n"
                + "       useTicketCache=false\n"
                + "       debug=false\n"
                + "       refreshKrb5Config=true\n"
                + "       principal=\"" + KerberosTestUtils.getLearnerPrincipal() + "\";\n" + "};\n";
        setupJaasConfig(jaasEntries);
    }

    @Before
    public void setUp() throws Exception {
                keytabFile = new File(KerberosTestUtils.getKeytabFile());
        String learnerPrincipal = KerberosTestUtils.getLearnerPrincipal();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        learnerPrincipal = learnerPrincipal.substring(0, learnerPrincipal.lastIndexOf("@"));
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        getKdc().createPrincipal(keytabFile, learnerPrincipal, serverPrincipal);
    }

    @After
    public void tearDown() throws Exception {
        for (MainThread mainThread : mt) {
            mainThread.shutdown();
            mainThread.deleteBaseDir();
        }
    }

    @AfterClass
    public static void cleanup() {
        if(keytabFile != null){
            FileUtils.deleteQuietly(keytabFile);
        }
        cleanupJaasConfig();
    }

    
    @Test(timeout = 120000)
    public void testValidCredentials() throws Exception {
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        Map<String, String> authConfigs = new HashMap<String, String>();
        authConfigs.put(QuorumAuth.QUORUM_SASL_AUTH_ENABLED, "true");
        authConfigs.put(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED, "true");
        authConfigs.put(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED, "true");
        authConfigs.put(QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL, serverPrincipal);
        String connectStr = startQuorum(3, authConfigs, 3);
        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeper zk = new ZooKeeper(connectStr, ClientBase.CONNECTION_TIMEOUT, watcher);
        watcher.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        for (int i = 0; i < 10; i++) {
            zk.create("/" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.close();
    }
}
