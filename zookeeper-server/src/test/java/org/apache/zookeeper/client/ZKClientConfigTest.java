package org.apache.zookeeper.client;

import static org.apache.zookeeper.client.ZKClientConfig.DISABLE_AUTO_WATCH_RESET;
import static org.apache.zookeeper.client.ZKClientConfig.ENABLE_CLIENT_SASL_KEY;
import static org.apache.zookeeper.client.ZKClientConfig.LOGIN_CONTEXT_NAME_KEY;
import static org.apache.zookeeper.client.ZKClientConfig.SECURE_CLIENT;
import static org.apache.zookeeper.client.ZKClientConfig.ZK_SASL_CLIENT_USERNAME;
import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET;
import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_SERVER_REALM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ZKClientConfigTest {
    private static final File testData = new File(System.getProperty("test.data.dir", "src/test/resources/data"));
    @Rule
    public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

    @BeforeClass
    public static void init() {
        if (!testData.exists()) {
            testData.mkdirs();
        }
    }

    @Test
    public void testDefaultConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(ZK_SASL_CLIENT_USERNAME, "zookeeper1");
        properties.put(LOGIN_CONTEXT_NAME_KEY, "Client1");
        properties.put(ENABLE_CLIENT_SASL_KEY, "true");
        properties.put(ZOOKEEPER_SERVER_REALM, "zookeeper/hadoop.hadoop.com");
        properties.put(DISABLE_AUTO_WATCH_RESET, "true");
        properties.put(ZOOKEEPER_CLIENT_CNXN_SOCKET, "ClientCnxnSocketNetty");
        properties.put(SECURE_CLIENT, "true");

        for (Map.Entry<String, String> e : properties.entrySet()) {
            System.setProperty(e.getKey(), e.getValue());
        }
        
        ZKClientConfig conf = new ZKClientConfig();
        for (Map.Entry<String, String> e : properties.entrySet()) {
            assertEquals(e.getValue(), conf.getProperty(e.getKey()));
        }
        
        for (Map.Entry<String, String> e : properties.entrySet()) {
            System.clearProperty(e.getKey());
        }

        conf = new ZKClientConfig();
        
        for (Map.Entry<String, String> e : properties.entrySet()) {
            String result = conf.getProperty(e.getKey());
            assertNull(result);
        }
    }

    @Test
    public void testSystemPropertyValue() {
        String clientName = "zookeeper1";
        System.setProperty(ZK_SASL_CLIENT_USERNAME, clientName);

        ZKClientConfig conf = new ZKClientConfig();
        assertEquals(conf.getProperty(ZK_SASL_CLIENT_USERNAME), clientName);

        String newClientName = "zookeeper2";
        conf.setProperty(ZK_SASL_CLIENT_USERNAME, newClientName);

        assertEquals(conf.getProperty(ZK_SASL_CLIENT_USERNAME), newClientName);
    }

    @Test
    public void testReadConfigurationFile() throws IOException, ConfigException {
        File file = File.createTempFile("clientConfig", ".conf", testData);
        file.deleteOnExit();
        Properties clientConfProp = new Properties();
        clientConfProp.setProperty(ENABLE_CLIENT_SASL_KEY, "true");
        clientConfProp.setProperty(ZK_SASL_CLIENT_USERNAME, "ZK");
        clientConfProp.setProperty(LOGIN_CONTEXT_NAME_KEY, "MyClient");
        clientConfProp.setProperty(ZOOKEEPER_SERVER_REALM, "HADOOP.COM");
        clientConfProp.setProperty("dummyProperty", "dummyValue");
        OutputStream io = new FileOutputStream(file);
        try {
            clientConfProp.store(io, "Client Configurations");
        } finally {
            io.close();
        }

        ZKClientConfig conf = new ZKClientConfig();
        conf.addConfiguration(file.getAbsolutePath());
        assertEquals(conf.getProperty(ENABLE_CLIENT_SASL_KEY), "true");
        assertEquals(conf.getProperty(ZK_SASL_CLIENT_USERNAME), "ZK");
        assertEquals(conf.getProperty(LOGIN_CONTEXT_NAME_KEY), "MyClient");
        assertEquals(conf.getProperty(ZOOKEEPER_SERVER_REALM), "HADOOP.COM");
        assertEquals(conf.getProperty("dummyProperty"), "dummyValue");

                        file.delete();

    }

    @Test
    public void testSetConfiguration() {
        ZKClientConfig conf = new ZKClientConfig();
        String defaultValue = conf.getProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                ZKClientConfig.ENABLE_CLIENT_SASL_DEFAULT);
        if (defaultValue.equals("true")) {
            conf.setProperty(ENABLE_CLIENT_SASL_KEY, "false");
        } else {
            conf.setProperty(ENABLE_CLIENT_SASL_KEY, "true");
        }
        assertTrue(conf.getProperty(ENABLE_CLIENT_SASL_KEY) != defaultValue);
    }

    @Test
    public void testIntegerRetrievalFromProperty() {
        ZKClientConfig conf = new ZKClientConfig();
        String prop = "UnSetProperty" + System.currentTimeMillis();
        int defaultValue = 100;
                int result = conf.getInt(prop, defaultValue);
        assertEquals(defaultValue, result);

                        conf.setProperty(ZKConfig.JUTE_MAXBUFFER, "InvlaidIntValue123");
        try {
            result = conf.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
            fail("NumberFormatException is expected");
        } catch (NumberFormatException exception) {
                    }
        assertEquals(defaultValue, result);

                int value = ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;
        conf.setProperty(ZKConfig.JUTE_MAXBUFFER, Integer.toString(value));
        result = conf.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
        assertEquals(value, result);

                value = 12345;
        conf.setProperty(ZKConfig.JUTE_MAXBUFFER,
                " " + Integer.toString(value) + " ");
        result = conf.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
        assertEquals(value, result);
    }

    @Test
    public void testIntegerRetrievalFromHexadecimalProperty() {
        int hexaValue = 0x3000000;
        String wrongValue = "0xwel";
        int defaultValue = 100;
                ZKClientConfig zkClientConfig = new ZKClientConfig();
        zkClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER,
                Integer.toString(hexaValue));
        int result = zkClientConfig.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
        assertEquals(result, hexaValue);
        zkClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER,
                wrongValue);
        try {
            result = zkClientConfig.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
            fail("NumberFormatException is expected");
        } catch (NumberFormatException exception) {
                    }
        zkClientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER,
                " " + hexaValue + " ");
        result = zkClientConfig.getInt(ZKConfig.JUTE_MAXBUFFER, defaultValue);
        assertEquals(result, hexaValue);
    }

}
