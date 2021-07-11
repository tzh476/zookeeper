package org.apache.zookeeper.test;


import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
public class SaslClientTest extends ZKTestCase {

    private String existingPropertyValue = null;

    @Before
    public void setUp() {
        existingPropertyValue = System
                .getProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY);
    }

    @After
    public void tearDown() {
                if (existingPropertyValue != null) {
            System.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                    existingPropertyValue);
        }
    }

    @Test
    public void testSaslClientDisabled() {
        System.clearProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY);
        Assert.assertTrue("SASL client disabled",
                new ZKClientConfig().isSaslClientEnabled());

        for (String value : Arrays.asList("true", "TRUE")) {
            System.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                    value);
            Assert.assertTrue("SASL client disabled",
                    new ZKClientConfig().isSaslClientEnabled());
        }

        for (String value : Arrays.asList("false", "FALSE")) {
            System.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                    value);
            Assert.assertFalse("SASL client disabled",
                    new ZKClientConfig().isSaslClientEnabled());
        }
    }
}
