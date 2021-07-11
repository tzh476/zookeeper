package org.apache.zookeeper;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.test.TestByteBufAllocator;
import org.apache.zookeeper.common.ZKConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientCnxnSocketTest {
    @Before
    public void setUp() {
        ClientCnxnSocketNetty.setTestAllocator(TestByteBufAllocator.getInstance());
    }

    @After
    public void tearDown() {
        ClientCnxnSocketNetty.clearTestAllocator();
        TestByteBufAllocator.checkForLeaks();
    }

    @Test
    public void testWhenInvalidJuteMaxBufferIsConfiguredIOExceptionIsThrown() {
        ZKClientConfig clientConfig = new ZKClientConfig();
        String value = "SomeInvalidInt";
        clientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, value);
                try {
            new ClientCnxnSocketNIO(clientConfig);
            fail("IOException is expected.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(value));
        }
                try {
            new ClientCnxnSocketNetty(clientConfig);
            fail("IOException is expected.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains(value));
        }

    }

}
