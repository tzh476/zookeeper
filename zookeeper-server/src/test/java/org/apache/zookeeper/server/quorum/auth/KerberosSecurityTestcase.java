package org.apache.zookeeper.server.quorum.auth;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Properties;




public class KerberosSecurityTestcase extends QuorumAuthTestBase {
    private static MiniKdc kdc;
    private static File workDir;
    private static Properties conf;

    @BeforeClass
    public static void setUpSasl() throws Exception {
        startMiniKdc();
    }

    @AfterClass
    public static void tearDownSasl() throws Exception {
        stopMiniKdc();
        FileUtils.deleteQuietly(workDir);
    }

    public static void startMiniKdc() throws Exception {
        createTestDir();
        createMiniKdcConf();

        kdc = new MiniKdc(conf, workDir);
        kdc.start();
    }

    
    public static void createTestDir() throws IOException {
        workDir = createTmpDir(
                new File(System.getProperty("build.test.dir", "build")));
    }

    static File createTmpDir(File parentDir) throws IOException {
        File tmpFile = File.createTempFile("test", ".junit", parentDir);
                        File tmpDir = new File(tmpFile + ".dir");
                Assert.assertFalse(tmpDir.exists());
        Assert.assertTrue(tmpDir.mkdirs());
        return tmpDir;
    }

    
    public static void createMiniKdcConf() {
        conf = MiniKdc.createConf();
    }

    public static void stopMiniKdc() {
        if (kdc != null) {
            kdc.stop();
        }
    }

    public static MiniKdc getKdc() {
        return kdc;
    }

    public static File getWorkDir() {
        return workDir;
    }

    public static Properties getConf() {
        return conf;
    }
}
