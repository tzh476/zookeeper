package org.apache.zookeeper.common;


import static org.junit.Assert.assertEquals;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;



public class ZKConfigTest {

    X509Util x509Util = new ClientX509Util();

    @Rule
    public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

    @After
    public void tearDown() throws Exception {
        System.clearProperty(x509Util.getSslProtocolProperty());
    }

        @Test
    public void testBooleanRetrievalFromPropertyDefault() {
        ZKConfig conf = new ZKConfig();
        String prop = "UnSetProperty" + System.currentTimeMillis();
        boolean defaultValue = false;
        boolean result = conf.getBoolean(prop, defaultValue);
        assertEquals(defaultValue, result);
    }

        @Test
    public void testBooleanRetrievalFromProperty() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), Boolean.toString(value));
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

        @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesInBeginning() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), " " + value);
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

        @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesAtEnd() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), value + " ");
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }

        @Test
    public void testBooleanRetrievalFromPropertyWithWhitespacesAtBeginningAndEnd() {
        boolean value = true;
        boolean defaultValue = false;
        System.setProperty(x509Util.getSslProtocolProperty(), " " + value + " ");
        ZKConfig conf = new ZKConfig();
        boolean result = conf.getBoolean(x509Util.getSslProtocolProperty(), defaultValue);
        assertEquals(value, result);
    }
}
