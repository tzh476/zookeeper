package org.apache.zookeeper.server.quorum;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.KeeperException.UnknownSessionException;
import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LearnerSessionTracker extends UpgradeableSessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerSessionTracker.class);

    private final SessionExpirer expirer;
        private final AtomicReference<Map<Long, Integer>> touchTable =
        new AtomicReference<Map<Long, Integer>>();
    private final long serverId;
    private final AtomicLong nextSessionId = new AtomicLong();

    private final boolean localSessionsEnabled;
    private final ConcurrentMap<Long, Integer> globalSessionsWithTimeouts;

    public LearnerSessionTracker(SessionExpirer expirer,
            ConcurrentMap<Long, Integer> sessionsWithTimeouts,
            int tickTime, long id, boolean localSessionsEnabled,
            ZooKeeperServerListener listener) {
        this.expirer = expirer;
        this.touchTable.set(new ConcurrentHashMap<Long, Integer>());
        this.globalSessionsWithTimeouts = sessionsWithTimeouts;
        this.serverId = id;
        nextSessionId.set(SessionTrackerImpl.initializeNextSession(serverId));

        this.localSessionsEnabled = localSessionsEnabled;
        if (this.localSessionsEnabled) {
            createLocalSessionTracker(expirer, tickTime, id, listener);
        }
    }

    public void removeSession(long sessionId) {
        if (localSessionTracker != null) {
            localSessionTracker.removeSession(sessionId);
        }
        globalSessionsWithTimeouts.remove(sessionId);
        touchTable.get().remove(sessionId);
    }

    public void start() {
        if (localSessionTracker != null) {
            localSessionTracker.start();
        }
    }

    public void shutdown() {
        if (localSessionTracker != null) {
            localSessionTracker.shutdown();
        }
    }

    public boolean isGlobalSession(long sessionId) {
        return globalSessionsWithTimeouts.containsKey(sessionId);
    }

    public boolean addGlobalSession(long sessionId, int sessionTimeout) {
        boolean added =
            globalSessionsWithTimeouts.put(sessionId, sessionTimeout) == null;
        if (localSessionsEnabled && added) {
                                    LOG.info("Adding global session 0x" + Long.toHexString(sessionId));
        }
        touchTable.get().put(sessionId, sessionTimeout);
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
                LOG.info("Adding local session 0x"
                         + Long.toHexString(sessionId));
            }
        } else {
            added = addGlobalSession(sessionId, sessionTimeout);
        }
        return added;
    }

    public boolean touchSession(long sessionId, int sessionTimeout) {
        if (localSessionsEnabled) {
            if (localSessionTracker.touchSession(sessionId, sessionTimeout)) {
                return true;
            }
            if (!isGlobalSession(sessionId)) {
                return false;
            }
        }
        touchTable.get().put(sessionId, sessionTimeout);
        return true;
    }

    public Map<Long, Integer> snapshot() {
        return touchTable.getAndSet(new ConcurrentHashMap<Long, Integer>());
    }

    public long createSession(int sessionTimeout) {
        if (localSessionsEnabled) {
            return localSessionTracker.createSession(sessionTimeout);
        }
        return nextSessionId.getAndIncrement();
    }

    public void checkSession(long sessionId, Object owner)
            throws SessionExpiredException, SessionMovedException  {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.checkSession(sessionId, owner);
                return;
            } catch (UnknownSessionException e) {
                                                                                if (!isGlobalSession(sessionId)) {
                    throw new SessionExpiredException();
                }
            }
        }
    }

    public void setOwner(long sessionId, Object owner)
            throws SessionExpiredException {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.setOwner(sessionId, owner);
                return;
            } catch (SessionExpiredException e) {
                                                                                if (!isGlobalSession(sessionId)) {
                    throw e;
                }
            }
        }
    }

    public void dumpSessions(PrintWriter pwriter) {
        if (localSessionTracker != null) {
            pwriter.print("Local ");
            localSessionTracker.dumpSessions(pwriter);
        }
        pwriter.print("Global Sessions(");
        pwriter.print(globalSessionsWithTimeouts.size());
        pwriter.println("):");
        SortedSet<Long> sessionIds = new TreeSet<Long>(
                globalSessionsWithTimeouts.keySet());
        for (long sessionId : sessionIds) {
            pwriter.print("0x");
            pwriter.print(Long.toHexString(sessionId));
            pwriter.print("\t");
            pwriter.print(globalSessionsWithTimeouts.get(sessionId));
            pwriter.println("ms");
        }
    }

    public void setSessionClosing(long sessionId) {
                        if (localSessionTracker != null) {
            localSessionTracker.setSessionClosing(sessionId);
        }
    }

    @Override
    public Map<Long, Set<Long>> getSessionExpiryMap() {
        return new HashMap<Long, Set<Long>>();
    }
}
