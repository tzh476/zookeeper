package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.Test;

public class QuorumPeerConfigTest {

    
    @Test
    public void testErrorMessageWhensecureClientPortNotSetButsecureClientPortAddressSet()
            throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            Properties zkProp = getDefaultZKProperties();
            zkProp.setProperty("secureClientPortAddress", "localhost");
            quorumPeerConfig.parseProperties(zkProp);
            fail("IllegalArgumentException is expected");
        } catch (IllegalArgumentException e) {
            String expectedMessage = "secureClientPortAddress is set but secureClientPort is not set";
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    
    @Test
    public void testErrorMessageWhenclientPortNotSetButclientPortAddressSet()
            throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            Properties zkProp = getDefaultZKProperties();
            zkProp.setProperty("clientPortAddress", "localhost");
            quorumPeerConfig.parseProperties(zkProp);
            fail("IllegalArgumentException is expected");
        } catch (IllegalArgumentException e) {
            String expectedMessage = "clientPortAddress is set but clientPort is not set";
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    
    @Test
    public void testConfigureSSLAuthGetsConfiguredIfSecurePortConfigured()
            throws IOException, ConfigException {
        String sslAuthProp = "zookeeper.authProvider.x509";
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        Properties zkProp = getDefaultZKProperties();
        zkProp.setProperty("secureClientPort", "12345");
        quorumPeerConfig.parseProperties(zkProp);
        String expected = "org.apache.zookeeper.server.auth.X509AuthenticationProvider";
        String result = System.getProperty(sslAuthProp);
        assertEquals(expected, result); 
    }

    
    @Test
    public void testCustomSSLAuth() throws IOException {
        try (ClientX509Util x509Util = new ClientX509Util()) {
            System.setProperty(x509Util.getSslAuthProviderProperty(), "y509");
            QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
            try {
                Properties zkProp = getDefaultZKProperties();
                zkProp.setProperty("secureClientPort", "12345");
                quorumPeerConfig.parseProperties(zkProp);
                fail("ConfigException is expected");
            } catch (ConfigException e) {
                assertNotNull(e.getMessage());
            }
        }
    }

    
    @Test(expected = ConfigException.class)
    public void testSamePortConfiguredForClientAndElection() throws IOException, ConfigException {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        Properties zkProp = getDefaultZKProperties();
        zkProp.setProperty("server.1", "localhost:2888:2888");
        quorumPeerConfig.parseProperties(zkProp);
    }

    private Properties getDefaultZKProperties() {
        Properties zkProp = new Properties();
        zkProp.setProperty("dataDir", new File("myDataDir").getAbsolutePath());
        return zkProp;
    }

}
