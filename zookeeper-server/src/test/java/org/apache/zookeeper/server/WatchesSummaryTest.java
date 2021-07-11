package org.apache.zookeeper.server;

import java.util.Map;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class WatchesSummaryTest extends ZKTestCase {
    private WatchesSummary s;
    @Before public void setUp() {
        s = new WatchesSummary(1, 2, 3);
    }
    @Test public void testGetters() {
        assertEquals(1, s.getNumConnections());
        assertEquals(2, s.getNumPaths());
        assertEquals(3, s.getTotalWatches());
    }
    @Test public void testToMap() {
        Map<String, Object> m = s.toMap();
        assertEquals(3, m.size());
        assertEquals(Integer.valueOf(1), m.get(WatchesSummary.KEY_NUM_CONNECTIONS));
        assertEquals(Integer.valueOf(2), m.get(WatchesSummary.KEY_NUM_PATHS));
        assertEquals(Integer.valueOf(3), m.get(WatchesSummary.KEY_NUM_TOTAL_WATCHES));
    }
}
