package org.apache.zookeeper.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class WatchesReportTest extends ZKTestCase {
    private Map<Long, Set<String>> m;
    private WatchesReport r;
    @Before public void setUp() {
        m = new HashMap<Long, Set<String>>();
        Set<String> s = new HashSet<String>();
        s.add("path1a");
        s.add("path1b");
        m.put(1L, s);
        s = new HashSet<String>();
        s.add("path2a");
        m.put(2L, s);
        r = new WatchesReport(m);
    }
    @Test public void testHasPaths() {
        assertTrue(r.hasPaths(1L));
        assertTrue(r.hasPaths(2L));
        assertFalse(r.hasPaths(3L));
    }
    @Test public void testGetPaths() {
        Set<String> s = r.getPaths(1L);
        assertEquals(2, s.size());
        assertTrue(s.contains("path1a"));
        assertTrue(s.contains("path1b"));
        s = r.getPaths(2L);
        assertEquals(1, s.size());
        assertTrue(s.contains("path2a"));
        assertNull(r.getPaths(3L));
    }
    @Test public void testToMap() {
        assertEquals(m, r.toMap());
    }
}
