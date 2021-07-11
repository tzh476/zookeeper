package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class WatchesReport {

    private final Map<Long, Set<String>> id2paths;

    
    WatchesReport(Map<Long, Set<String>> id2paths) {
        this.id2paths = Collections.unmodifiableMap(deepCopy(id2paths));
    }

    private static Map<Long, Set<String>> deepCopy(Map<Long, Set<String>> m) {
        Map<Long, Set<String>> m2 = new HashMap<Long, Set<String>>();
        for (Map.Entry<Long, Set<String>> e : m.entrySet()) {
            m2.put(e.getKey(), new HashSet<String>(e.getValue()));
        }
        return m2;
    }

    
    public boolean hasPaths(long sessionId) {
        return id2paths.containsKey(sessionId);
    }

    
    public Set<String> getPaths(long sessionId) {
        Set<String> s = id2paths.get(sessionId);
        return s != null ? Collections.unmodifiableSet(s) : null;
    }

    
    public Map<Long, Set<String>> toMap() {
        return deepCopy(id2paths);
    }
}
