package org.apache.zookeeper.server.quorum;

import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;


public class LocalSessionTracker extends SessionTrackerImpl {
    public LocalSessionTracker(SessionExpirer expirer,
            ConcurrentMap<Long, Integer> sessionsWithTimeouts,
            int tickTime, long id, ZooKeeperServerListener listener) {
        super(expirer, sessionsWithTimeouts, tickTime, id, listener);
    }

    public boolean isLocalSession(long sessionId) {
        return isTrackingSession(sessionId);
    }

    public boolean isGlobalSession(long sessionId) {
        return false;
    }

    public boolean addGlobalSession(long sessionId, int sessionTimeout) {
        throw new UnsupportedOperationException();
    }
}
