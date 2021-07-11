package org.apache.zookeeper.server;

import java.util.Map;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ZooKeeperServerConfTest extends ZKTestCase {
    private ZooKeeperServerConf c;
    @Before public void setUp() {
        c = new ZooKeeperServerConf(1, "a", "b", 2, 3, 4, 5, 6L);
    }
    @Test public void testGetters() {
        assertEquals(1, c.getClientPort());
        assertEquals("a", c.getDataDir());
        assertEquals("b", c.getDataLogDir());
        assertEquals(2, c.getTickTime());
        assertEquals(3, c.getMaxClientCnxnsPerHost());
        assertEquals(4, c.getMinSessionTimeout());
        assertEquals(5, c.getMaxSessionTimeout());
        assertEquals(6L, c.getServerId());
    }
    @Test public void testToMap() {
        Map<String, Object> m = c.toMap();
        assertEquals(8, m.size());
        assertEquals(Integer.valueOf(1), m.get(ZooKeeperServerConf.KEY_CLIENT_PORT));
        assertEquals("a", m.get(ZooKeeperServerConf.KEY_DATA_DIR));
        assertEquals("b", m.get(ZooKeeperServerConf.KEY_DATA_LOG_DIR));
        assertEquals(Integer.valueOf(2), m.get(ZooKeeperServerConf.KEY_TICK_TIME));
        assertEquals(Integer.valueOf(3), m.get(ZooKeeperServerConf.KEY_MAX_CLIENT_CNXNS));
        assertEquals(Integer.valueOf(4), m.get(ZooKeeperServerConf.KEY_MIN_SESSION_TIMEOUT));
        assertEquals(Integer.valueOf(5), m.get(ZooKeeperServerConf.KEY_MAX_SESSION_TIMEOUT));
        assertEquals(Long.valueOf(6L), m.get(ZooKeeperServerConf.KEY_SERVER_ID));
    }
}
