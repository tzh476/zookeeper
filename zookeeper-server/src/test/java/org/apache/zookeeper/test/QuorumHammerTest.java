package org.apache.zookeeper.test;
import org.apache.zookeeper.ZKTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumHammerTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(QuorumHammerTest.class);
    public static final long CONNECTION_TIMEOUT = ClientTest.CONNECTION_TIMEOUT;

    protected final QuorumBase qb = new QuorumBase();
    protected final ClientHammerTest cht = new ClientHammerTest();

    @Before
    public void setUp() throws Exception {
        qb.setUp();
        cht.hostPort = qb.hostPort;
        cht.setUpAll();
    }

    @After
    public void tearDown() throws Exception {
        cht.tearDownAll();
        qb.tearDown();
    }

    @Test
    public void testHammerBasic() throws Throwable {
        cht.testHammerBasic();
    }
}
