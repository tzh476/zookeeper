package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class WatchesPathReport {

    private final Map<String, Set<Long>> path2Ids;

    
    WatchesPathReport(Map<String, Set<Long>> path2Ids) {
        this.path2Ids = Collections.unmodifiableMap(deepCopy(path2Ids));
    }

    private static Map<String, Set<Long>> deepCopy(Map<String, Set<Long>> m) {
        Map<String, Set<Long>> m2 = new HashMap<String, Set<Long>>();
        for (Map.Entry<String, Set<Long>> e : m.entrySet()) {
            m2.put(e.getKey(), new HashSet<Long>(e.getValue()));
        }
        return m2;
    }

    
    public boolean hasSessions(String path) {
        return path2Ids.containsKey(path);
    }
    
    public Set<Long> getSessions(String path) {
        Set<Long> s = path2Ids.get(path);
        return s != null ? Collections.unmodifiableSet(s) : null;
    }

    
    public Map<String, Set<Long>> toMap() {
        return deepCopy(path2Ids);
    }
}
