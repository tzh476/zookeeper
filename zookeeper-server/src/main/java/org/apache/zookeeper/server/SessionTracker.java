package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;


public interface SessionTracker {
    public static interface Session {
        long getSessionId();
        int getTimeout();
        boolean isClosing();
    }
    public static interface SessionExpirer {
        void expire(Session session);

        long getServerId();
    }

    long createSession(int sessionTimeout);

    
    boolean addGlobalSession(long id, int to);

    
    boolean addSession(long id, int to);

    
    boolean touchSession(long sessionId, int sessionTimeout);

    
    void setSessionClosing(long sessionId);

    
    void shutdown();

    
    void removeSession(long sessionId);

    
    boolean isTrackingSession(long sessionId);

    
    public void checkSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException,
            KeeperException.UnknownSessionException;

    
    public void checkGlobalSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException;

    void setOwner(long id, Object owner) throws SessionExpiredException;

    
    void dumpSessions(PrintWriter pwriter);

    
    Map<Long, Set<Long>> getSessionExpiryMap();
}
