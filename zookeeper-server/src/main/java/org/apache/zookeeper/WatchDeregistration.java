package org.apache.zookeeper;

import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper.ZKWatchManager;


public class WatchDeregistration {

    private final String clientPath;
    private final Watcher watcher;
    private final WatcherType watcherType;
    private final boolean local;
    private final ZKWatchManager zkManager;

    public WatchDeregistration(String clientPath, Watcher watcher,
            WatcherType watcherType, boolean local, ZKWatchManager zkManager) {
        this.clientPath = clientPath;
        this.watcher = watcher;
        this.watcherType = watcherType;
        this.local = local;
        this.zkManager = zkManager;
    }

    
    public Map<EventType, Set<Watcher>> unregister(int rc)
            throws KeeperException {
        return zkManager.removeWatcher(clientPath, watcher, watcherType, local,
                rc);
    }

    
    public String getClientPath() {
        return clientPath;
    }
}
