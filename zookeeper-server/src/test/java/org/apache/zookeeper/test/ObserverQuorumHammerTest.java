package org.apache.zookeeper.test;
import org.junit.Before;
import org.junit.Test;

public class ObserverQuorumHammerTest extends QuorumHammerTest {
    public static final long CONNECTION_TIMEOUT = ClientTest.CONNECTION_TIMEOUT;

    
    @Before
    @Override
    public void setUp() throws Exception {
        qb.setUp(true);
        cht.hostPort = qb.hostPort;
        cht.setUpAll();
    }
   
    @Test
    public void testHammerBasic() throws Throwable {
        cht.testHammerBasic();
    }
}
