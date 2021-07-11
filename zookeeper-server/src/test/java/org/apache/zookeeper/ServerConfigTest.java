package org.apache.zookeeper;

import org.apache.zookeeper.server.ServerConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

public class ServerConfigTest {

    private ServerConfig serverConfig;

    @Before
    public void setUp() {
        serverConfig = new ServerConfig();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testFewArguments() {
        String[] args = {"2181"};
        serverConfig.parse(args);
    }

    @Test
    public void testValidArguments() {
        String[] args = {"2181", "/data/dir", "60000", "10000"};
        serverConfig.parse(args);

        assertEquals(2181, serverConfig.getClientPortAddress().getPort());
        assertTrue(checkEquality("/data/dir", serverConfig.getDataDir()));
        assertEquals(60000, serverConfig.getTickTime());
        assertEquals(10000, serverConfig.getMaxClientCnxns());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testTooManyArguments() {
        String[] args = {"2181", "/data/dir", "60000", "10000", "9999"};
        serverConfig.parse(args);
    }

    boolean checkEquality(String a, String b) {
        assertNotNull(a);
        assertNotNull(b);
        return a.equals(b);
    }

    boolean checkEquality(String a, File b) {
        assertNotNull(a);
        assertNotNull(b);
        return new File(a).equals(b);
    }
}