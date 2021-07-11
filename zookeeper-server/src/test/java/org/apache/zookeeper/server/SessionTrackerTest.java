package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.SessionTrackerImpl.SessionImpl;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;


public class SessionTrackerTest extends ZKTestCase {

    private final long sessionId = 339900;
    private final int sessionTimeout = 3000;
    private FirstProcessor firstProcessor;
    private CountDownLatch latch;

    
    @Test(timeout = 20000)
    public void testAddSessionAfterSessionExpiry() throws Exception {
        ZooKeeperServer zks = setupSessionTracker();

        latch = new CountDownLatch(1);
        zks.sessionTracker.addSession(sessionId, sessionTimeout);
        SessionTrackerImpl sessionTrackerImpl = (SessionTrackerImpl) zks.sessionTracker;
        SessionImpl sessionImpl = sessionTrackerImpl.sessionsById
                .get(sessionId);
        Assert.assertNotNull("Sessionid:" + sessionId
                + " doesn't exists in sessiontracker", sessionImpl);

                Object sessionOwner = new Object();
        sessionTrackerImpl.checkSession(sessionId, sessionOwner);

                latch.await(sessionTimeout * 2, TimeUnit.MILLISECONDS);

                                sessionTrackerImpl.addSession(sessionId, sessionTimeout);
        try {
            sessionTrackerImpl.checkSession(sessionId, sessionOwner);
            Assert.fail("Should throw session expiry exception "
                    + "as the session has expired and closed");
        } catch (KeeperException.SessionExpiredException e) {
                    }
        Assert.assertTrue("Session didn't expired", sessionImpl.isClosing());
        Assert.assertFalse("Session didn't expired", sessionTrackerImpl
                .touchSession(sessionId, sessionTimeout));
        Assert.assertEquals(
                "Duplicate session expiry request has been generated", 1,
                firstProcessor.getCountOfCloseSessionReq());
    }

    
    @Test(timeout = 20000)
    public void testCloseSessionRequestAfterSessionExpiry() throws Exception {
        ZooKeeperServer zks = setupSessionTracker();

        latch = new CountDownLatch(1);
        zks.sessionTracker.addSession(sessionId, sessionTimeout);
        SessionTrackerImpl sessionTrackerImpl = (SessionTrackerImpl) zks.sessionTracker;
        SessionImpl sessionImpl = sessionTrackerImpl.sessionsById
                .get(sessionId);
        Assert.assertNotNull("Sessionid:" + sessionId
                + " doesn't exists in sessiontracker", sessionImpl);

                Object sessionOwner = new Object();
        sessionTrackerImpl.checkSession(sessionId, sessionOwner);

                latch.await(sessionTimeout * 2, TimeUnit.MILLISECONDS);

                        sessionTrackerImpl.removeSession(sessionId);
        SessionImpl actualSession = sessionTrackerImpl.sessionsById
                .get(sessionId);
        Assert.assertNull("Session:" + sessionId
                + " still exists after removal", actualSession);
    }

    private ZooKeeperServer setupSessionTracker() throws IOException {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        zks.setupRequestProcessors();
        firstProcessor = new FirstProcessor(zks, null);
        zks.firstProcessor = firstProcessor;

                zks.createSessionTracker();
        zks.startSessionTracker();
        return zks;
    }

        private class FirstProcessor extends PrepRequestProcessor {
        private volatile int countOfCloseSessionReq = 0;

        public FirstProcessor(ZooKeeperServer zks,
                RequestProcessor nextProcessor) {
            super(zks, nextProcessor);
        }

        @Override
        public void processRequest(Request request) {
                        if (request.type == OpCode.closeSession) {
                countOfCloseSessionReq++;
                latch.countDown();
            }
        }

                int getCountOfCloseSessionReq() {
            return countOfCloseSessionReq;
        }
    }
}
