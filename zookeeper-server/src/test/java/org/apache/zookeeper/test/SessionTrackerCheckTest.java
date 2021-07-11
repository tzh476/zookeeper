package org.apache.zookeeper.test;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.SessionTracker.SessionExpirer;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.LearnerSessionTracker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SessionTrackerCheckTest extends ZKTestCase {

    protected static final Logger LOG = LoggerFactory
            .getLogger(SessionTrackerCheckTest.class);
    public static final int TICK_TIME = 1000;
    public static final int CONNECTION_TIMEOUT = TICK_TIME * 10;

    private ConcurrentHashMap<Long, Integer> sessionsWithTimeouts =
            new ConcurrentHashMap<Long, Integer>();

    private class Expirer implements SessionExpirer {
        long sid;

        public Expirer(long sid) {
            this.sid = sid;
        }

        public void expire(Session session) {
        }

        public long getServerId() {
            return sid;
        }
    }

    @Before
    public void setUp() throws Exception {
        sessionsWithTimeouts.clear();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testLearnerSessionTracker() throws Exception {
        Expirer expirer = new Expirer(1);
                LearnerSessionTracker tracker = new LearnerSessionTracker(expirer,
                sessionsWithTimeouts, TICK_TIME, expirer.sid, true,
                testZKSListener());

                long sessionId = 0xb100ded;
        try {
            tracker.checkSession(sessionId, null);
            Assert.fail("Unknown session should have failed");
        } catch (SessionExpiredException e) {
                    }

                sessionsWithTimeouts.put(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail");
        }

                sessionId = 0xf005ba11;
        tracker.addSession(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Local session should not fail");
        }

                sessionsWithTimeouts.put(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Session during upgrade should not fail");
        }

                tracker = new LearnerSessionTracker(expirer, sessionsWithTimeouts,
                TICK_TIME, expirer.sid, false, testZKSListener());

                sessionId = 0xdeadbeef;
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Should not get any exception");
        }

    }

    @Test
    public void testLeaderSessionTracker() throws Exception {
        Expirer expirer = new Expirer(2);
                LeaderSessionTracker tracker = new LeaderSessionTracker(expirer,
                sessionsWithTimeouts, TICK_TIME, expirer.sid, true,
                testZKSListener());

                long sessionId = ((expirer.sid + 1) << 56) + 1;
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("local session from other server should not fail");
        }

                tracker.addGlobalSession(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail");
        }
        try {
            tracker.checkGlobalSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail " + e);
        }

                sessionId = (expirer.sid << 56) + 1;
        ;
        tracker.addSession(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Local session on the leader should not fail");
        }

                tracker.addGlobalSession(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Session during upgrade should not fail");
        }
        try {
            tracker.checkGlobalSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail " + e);
        }

                tracker = new LeaderSessionTracker(expirer, sessionsWithTimeouts,
                TICK_TIME, expirer.sid, false, testZKSListener());

                sessionId = 0xdeadbeef;
        tracker.addSession(sessionId, CONNECTION_TIMEOUT);
        try {
            tracker.checkSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail");
        }
        try {
            tracker.checkGlobalSession(sessionId, null);
        } catch (Exception e) {
            Assert.fail("Global session should not fail");
        }

                sessionId = ((expirer.sid + 1) << 56) + 2;
        try {
            tracker.checkSession(sessionId, null);
            Assert.fail("local session from other server should fail");
        } catch (SessionExpiredException e) {
                    }

                sessionId = ((expirer.sid) << 56) + 2;
        try {
            tracker.checkSession(sessionId, null);
            Assert.fail("local session from the leader should fail");
        } catch (SessionExpiredException e) {
                    }

    }

    ZooKeeperServerListener testZKSListener() {
        return new ZooKeeperServerListener() {

            @Override
            public void notifyStopping(String errMsg, int exitCode) {

            }
        };
    }
}
