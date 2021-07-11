package org.apache.zookeeper.server;

import java.util.LinkedHashMap;
import java.util.Map;


public class WatchesSummary {

    
    public static final String KEY_NUM_CONNECTIONS = "num_connections";
    
    public static final String KEY_NUM_PATHS = "num_paths";
    
    public static final String KEY_NUM_TOTAL_WATCHES = "num_total_watches";

    private final int numConnections;
    private final int numPaths;
    private final int totalWatches;

    
    WatchesSummary(int numConnections, int numPaths, int totalWatches) {
        this.numConnections = numConnections;
        this.numPaths = numPaths;
        this.totalWatches = totalWatches;
    }

    
    public int getNumConnections() {
        return numConnections;
    }
    
    public int getNumPaths() {
        return numPaths;
    }
    
    public int getTotalWatches() {
        return totalWatches;
    }

    
    public Map<String, Object> toMap() {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put(KEY_NUM_CONNECTIONS, numConnections);
        summary.put(KEY_NUM_PATHS, numPaths);
        summary.put(KEY_NUM_TOTAL_WATCHES, totalWatches);
        return summary;
    }
}
