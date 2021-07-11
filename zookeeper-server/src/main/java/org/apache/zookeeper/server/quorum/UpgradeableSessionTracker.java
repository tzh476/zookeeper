package org.apache.zookeeper.server.quorum;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class UpgradeableSessionTracker implements SessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(UpgradeableSessionTracker.class);

    private ConcurrentMap<Long, Integer> localSessionsWithTimeouts;
    protected LocalSessionTracker localSessionTracker;

    public void start() {}

    public void createLocalSessionTracker(SessionExpirer expirer,
            int tickTime, long id, ZooKeeperServerListener listener) {
        this.localSessionsWithTimeouts =
            new ConcurrentHashMap<Long, Integer>();
        this.localSessionTracker = new LocalSessionTracker(
            expirer, this.localSessionsWithTimeouts, tickTime, id, listener);
    }

    public boolean isTrackingSession(long sessionId) {
        return isLocalSession(sessionId) || isGlobalSession(sessionId);
    }

    public boolean isLocalSession(long sessionId) {
        return localSessionTracker != null &&
            localSessionTracker.isTrackingSession(sessionId);
    }

    abstract public boolean isGlobalSession(long sessionId);

    
    public int upgradeSession(long sessionId) {
        if (localSessionsWithTimeouts == null) {
            return -1;
        }
                        Integer timeout = localSessionsWithTimeouts.remove(sessionId);
        if (timeout != null) {
            LOG.info("Upgrading session 0x" + Long.toHexString(sessionId));
                        addGlobalSession(sessionId, timeout);
            localSessionTracker.removeSession(sessionId);
            return timeout;
        }
        return -1;
    }

    public void checkGlobalSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException {
        throw new UnsupportedOperationException();
    }
}
