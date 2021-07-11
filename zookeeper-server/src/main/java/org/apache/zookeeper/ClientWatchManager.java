package org.apache.zookeeper;

import java.util.Set;


public interface ClientWatchManager {
    
    public Set<Watcher> materialize(Watcher.Event.KeeperState state,
        Watcher.Event.EventType type, String path);
}
