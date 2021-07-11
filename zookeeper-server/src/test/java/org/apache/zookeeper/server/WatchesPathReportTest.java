package org.apache.zookeeper.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class WatchesPathReportTest extends ZKTestCase {
    private Map<String, Set<Long>> m;
    private WatchesPathReport r;
    @Before public void setUp() {
        m = new HashMap<String, Set<Long>>();
        Set<Long> s = new HashSet<Long>();
        s.add(101L);
        s.add(102L);
        m.put("path1", s);
        s = new HashSet<Long>();
        s.add(201L);
        m.put("path2", s);
        r = new WatchesPathReport(m);
    }
    @Test public void testHasSessions() {
        assertTrue(r.hasSessions("path1"));
        assertTrue(r.hasSessions("path2"));
        assertFalse(r.hasSessions("path3"));
    }
    @Test public void testGetSessions() {
        Set<Long> s = r.getSessions("path1");
        assertEquals(2, s.size());
        assertTrue(s.contains(101L));
        assertTrue(s.contains(102L));
        s = r.getSessions("path2");
        assertEquals(1, s.size());
        assertTrue(s.contains(201L));
        assertNull(r.getSessions("path3"));
    }
    @Test public void testToMap() {
        assertEquals(m, r.toMap());
    }
}
