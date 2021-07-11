package org.apache.zookeeper.server.quorum;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.KeeperException.UnknownSessionException;
import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeaderSessionTracker extends UpgradeableSessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderSessionTracker.class);

    private final boolean localSessionsEnabled;
    private final SessionTrackerImpl globalSessionTracker;

    
    private final long serverId;

    public LeaderSessionTracker(SessionExpirer expirer,
            ConcurrentMap<Long, Integer> sessionsWithTimeouts,
            int tickTime, long id, boolean localSessionsEnabled,
            ZooKeeperServerListener listener) {

        this.globalSessionTracker = new SessionTrackerImpl(
            expirer, sessionsWithTimeouts, tickTime, id, listener);

        this.localSessionsEnabled = localSessionsEnabled;
        if (this.localSessionsEnabled) {
            createLocalSessionTracker(expirer, tickTime, id, listener);
        }
        serverId = id;
    }

    public void removeSession(long sessionId) {
        if (localSessionTracker != null) {
            localSessionTracker.removeSession(sessionId);
        }
        globalSessionTracker.removeSession(sessionId);
    }

    public void start() {
        globalSessionTracker.start();
        if (localSessionTracker != null) {
            localSessionTracker.start();
        }
    }

    public void shutdown() {
        if (localSessionTracker != null) {
            localSessionTracker.shutdown();
        }
        globalSessionTracker.shutdown();
    }

    public boolean isGlobalSession(long sessionId) {
        return globalSessionTracker.isTrackingSession(sessionId);
    }

    public boolean addGlobalSession(long sessionId, int sessionTimeout) {
        boolean added =
            globalSessionTracker.addSession(sessionId, sessionTimeout);
        if (localSessionsEnabled && added) {
                                    LOG.info("Adding global session 0x" + Long.toHexString(sessionId));
        }
        return added;
    }

    public boolean addSession(long sessionId, int sessionTimeout) {
        boolean added;
        if (localSessionsEnabled && !isGlobalSession(sessionId)) {
            added = localSessionTracker.addSession(sessionId, sessionTimeout);
                        if (isGlobalSession(sessionId)) {
                added = false;
                localSessionTracker.removeSession(sessionId);
            } else if (added) {
              LOG.info("Adding local session 0x" + Long.toHexString(sessionId));
            }
        } else {
            added = addGlobalSession(sessionId, sessionTimeout);
        }
        return added;
    }

    public boolean touchSession(long sessionId, int sessionTimeout) {
        if (localSessionTracker != null &&
            localSessionTracker.touchSession(sessionId, sessionTimeout)) {
            return true;
        }
        return globalSessionTracker.touchSession(sessionId, sessionTimeout);
    }

    public long createSession(int sessionTimeout) {
        if (localSessionsEnabled) {
            return localSessionTracker.createSession(sessionTimeout);
        }
        return globalSessionTracker.createSession(sessionTimeout);
    }

        public static long getServerIdFromSessionId(long sessionId) {
        return sessionId >> 56;
    }

    public void checkSession(long sessionId, Object owner)
            throws SessionExpiredException, SessionMovedException,
            UnknownSessionException {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.checkSession(sessionId, owner);
                                                if (!isGlobalSession(sessionId)) {
                    return;
                }
            } catch(UnknownSessionException e) {
                            }
        }
        try {
            globalSessionTracker.checkSession(sessionId, owner);
                        return;
        } catch (UnknownSessionException e) {
                    }

        
        if (!localSessionsEnabled
                || (getServerIdFromSessionId(sessionId) == serverId)) {
            throw new SessionExpiredException();
        }
    }

    public void checkGlobalSession(long sessionId, Object owner)
            throws SessionExpiredException, SessionMovedException {
        try {
            globalSessionTracker.checkSession(sessionId, owner);
        } catch (UnknownSessionException e) {
                        throw new SessionExpiredException();
        }
    }

    public void setOwner(long sessionId, Object owner)
            throws SessionExpiredException {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.setOwner(sessionId, owner);
                return;
            } catch(SessionExpiredException e) {
                            }
        }
        globalSessionTracker.setOwner(sessionId, owner);
    }

    public void dumpSessions(PrintWriter pwriter) {
      if (localSessionTracker != null) {
          pwriter.print("Local ");
          localSessionTracker.dumpSessions(pwriter);
          pwriter.print("Global ");
      }
      globalSessionTracker.dumpSessions(pwriter);
    }

    public void setSessionClosing(long sessionId) {
                if (localSessionTracker != null) {
            localSessionTracker.setSessionClosing(sessionId);
        }
        globalSessionTracker.setSessionClosing(sessionId);
    }

    public Map<Long, Set<Long>> getSessionExpiryMap() {
        Map<Long, Set<Long>> sessionExpiryMap;
                        if (localSessionTracker != null) {
            sessionExpiryMap = localSessionTracker.getSessionExpiryMap();
        } else {
            sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        }
        sessionExpiryMap.putAll(globalSessionTracker.getSessionExpiryMap());
        return sessionExpiryMap;
    }
}
