package org.apache.zookeeper.server.quorum;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.apache.zookeeper.test.ClientBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommitProcessorTest extends ZKTestCase {
    protected static final Logger LOG =
        LoggerFactory.getLogger(CommitProcessorTest.class);


    private AtomicInteger processedReadRequests = new AtomicInteger(0);
    private AtomicInteger processedWriteRequests = new AtomicInteger(0);

    TestZooKeeperServer zks;
    File tmpDir;
    ArrayList<TestClientThread> testClients =
        new ArrayList<TestClientThread>();

    public void setUp(int numCommitThreads, int numClientThreads)
            throws Exception {
        System.setProperty(
            CommitProcessor.ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS,
            Integer.toString(numCommitThreads));
        System.setProperty("zookeeper.admin.enableServer", "false");
        tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        zks = new TestZooKeeperServer(tmpDir, tmpDir, 4000);
        zks.startup();
        for(int i=0; i<numClientThreads; ++i) {
            TestClientThread client = new TestClientThread();
            testClients.add(client);
            client.start();
        }
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("tearDown starting");
        for(TestClientThread client : testClients) {
            client.interrupt();
            client.join();
        }
        zks.shutdown();

        if (tmpDir != null) {
            Assert.assertTrue("delete " + tmpDir.toString(),
                              ClientBase.recursiveDelete(tmpDir));
        }
    }

    private class TestClientThread extends Thread {
        long sessionId;
        int cxid;
        int nodeId;

        public TestClientThread() {
            sessionId = zks.getSessionTracker().createSession(5000);
        }

        public void sendWriteRequest() throws Exception {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(boas);
            CreateRequest createReq = new CreateRequest(
                "/session" + Long.toHexString(sessionId) + "-" + (++nodeId),
                new byte[0], Ids.OPEN_ACL_UNSAFE, 1);
            createReq.serialize(boa, "request");
            ByteBuffer bb = ByteBuffer.wrap(boas.toByteArray());
            Request req = new Request(null, sessionId, ++cxid, OpCode.create,
                                      bb, new ArrayList<Id>());
            zks.firstProcessor.processRequest(req);
        }

        public void sendReadRequest() throws Exception {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(boas);
            GetDataRequest getDataRequest = new GetDataRequest(
                "/session" + Long.toHexString(sessionId) + "-" + nodeId, false);
            getDataRequest.serialize(boa, "request");
            ByteBuffer bb = ByteBuffer.wrap(boas.toByteArray());
            Request req = new Request(null, sessionId, ++cxid, OpCode.getData,
                                      bb, new ArrayList<Id>());
            zks.firstProcessor.processRequest(req);
        }

        public void run() {
            Random rand = new Random(Thread.currentThread().getId());
            try {
                sendWriteRequest();
                for(int i=0; i<1000; ++i) {
                                        if (rand.nextInt(100) < 25) {
                        sendWriteRequest();
                    } else {
                        sendReadRequest();
                    }
                }
            } catch (Exception e) {
                LOG.error("Uncaught exception in test: ", e);
            }
        }
    }

    @Test
    public void testNoCommitWorkers() throws Exception {
        setUp(0, 10);
        synchronized(this) {
            wait(5000);
        }
        checkProcessedRequest();
        Assert.assertFalse(fail);
    }

    @Test
    public void testOneCommitWorker() throws Exception {
        setUp(1, 10);
        synchronized(this) {
            wait(5000);
        }
        checkProcessedRequest();
        Assert.assertFalse(fail);
    }

    @Test
    public void testManyCommitWorkers() throws Exception {
        setUp(10, 10);
        synchronized(this) {
            wait(5000);
        }
        checkProcessedRequest();
        Assert.assertFalse(fail);

    }

    private void checkProcessedRequest() {
        Assert.assertTrue("No read requests processed",
                processedReadRequests.get() > 0);
        Assert.assertTrue("No write requests processed",
                processedWriteRequests.get() > 0);
    }

    volatile boolean fail = false;
    synchronized private void failTest(String reason) {
        fail = true;
        notifyAll();
        Assert.fail(reason);
    }

    private class TestZooKeeperServer extends ZooKeeperServer {
        PrepRequestProcessor firstProcessor;
        CommitProcessor commitProcessor;

        public TestZooKeeperServer(File snapDir, File logDir, int tickTime)
                throws IOException {
            super(snapDir, logDir, tickTime);
        }

        public SessionTracker getSessionTracker() {
            return sessionTracker;
        }

                        @Override
        protected void setupRequestProcessors() {
            RequestProcessor finalProcessor = new FinalRequestProcessor(zks);
                                    ValidateProcessor validateProcessor =
                new ValidateProcessor(finalProcessor);
            commitProcessor = new CommitProcessor(validateProcessor, "1", true,
                    getZooKeeperServerListener());
            validateProcessor.setCommitProcessor(commitProcessor);
            commitProcessor.start();
            MockProposalRequestProcessor proposalProcessor =
                new MockProposalRequestProcessor(commitProcessor);
            proposalProcessor.start();
            firstProcessor = new PrepRequestProcessor(zks, proposalProcessor);
            firstProcessor.start();
        }
    }

    private class MockProposalRequestProcessor extends Thread
            implements RequestProcessor {
        private final CommitProcessor commitProcessor;
        private final LinkedBlockingQueue<Request> proposals =
            new LinkedBlockingQueue<Request>();

        public MockProposalRequestProcessor(CommitProcessor commitProcessor) {
            this.commitProcessor = commitProcessor;
        }

        @Override
        public void run() {
            Random rand = new Random(Thread.currentThread().getId());
            try {
                while(true) {
                    Request request = proposals.take();
                    Thread.sleep(10 + rand.nextInt(190));
                    commitProcessor.commit(request);
                }
            } catch (InterruptedException e) {
                            }
        }

        @Override
        public void processRequest(Request request)
                throws RequestProcessorException {
            commitProcessor.processRequest(request);
            if (request.getHdr() != null) {
                                proposals.add(request);
            }
        }

        @Override
        public void shutdown() {
            
        }
    }

    private class ValidateProcessor implements RequestProcessor {
        Random rand = new Random(Thread.currentThread().getId());
        RequestProcessor nextProcessor;
        CommitProcessor commitProcessor;
        AtomicLong expectedZxid = new AtomicLong(1);
        ConcurrentHashMap<Long, AtomicInteger> cxidMap =
            new ConcurrentHashMap<Long, AtomicInteger>();

        AtomicInteger outstandingReadRequests = new AtomicInteger(0);
        AtomicInteger outstandingWriteRequests = new AtomicInteger(0);

        public ValidateProcessor(RequestProcessor nextProcessor) {
            this.nextProcessor = nextProcessor;
        }

        public void setCommitProcessor(CommitProcessor commitProcessor) {
            this.commitProcessor = commitProcessor;
        }


        @Override
        public void processRequest(Request request)
                throws RequestProcessorException {
            boolean isWriteRequest = commitProcessor.needCommit(request);
            if (isWriteRequest) {
                outstandingWriteRequests.incrementAndGet();
                validateWriteRequestVariant(request);
                LOG.debug("Starting write request zxid=" + request.zxid);
            } else {
                LOG.debug("Starting read request cxid="
                        + request.cxid + " for session 0x"
                        + Long.toHexString(request.sessionId));
                outstandingReadRequests.incrementAndGet();
                validateReadRequestVariant(request);
            }

                        try {
                Thread.sleep(10 + rand.nextInt(290));
            } catch(InterruptedException e) {
                            }
            nextProcessor.processRequest(request);

            
            if (isWriteRequest) {
                outstandingWriteRequests.decrementAndGet();
                LOG.debug("Done write request zxid=" + request.zxid);
                processedWriteRequests.incrementAndGet();
            } else {
                outstandingReadRequests.decrementAndGet();
                LOG.debug("Done read request cxid="
                        + request.cxid + " for session 0x"
                        + Long.toHexString(request.sessionId));
                processedReadRequests.incrementAndGet();
            }
            validateRequest(request);
        }

        
        private void validateWriteRequestVariant(Request request) {
            long zxid = request.getHdr().getZxid();
            int readRequests = outstandingReadRequests.get();
            if (readRequests != 0) {
                failTest("There are " + readRequests + " outstanding"
                        + " read requests while issuing a write request zxid="
                        + zxid);
            }
            int writeRequests = outstandingWriteRequests.get();
            if (writeRequests > 1) {
                failTest("There are " + writeRequests + " outstanding"
                        + " write requests while issuing a write request zxid="
                        + zxid + " (expected one)");
            }
        }

        
        private void validateReadRequestVariant(Request request) {
            int writeRequests = outstandingWriteRequests.get();
            if (writeRequests != 0) {
                failTest("There are " + writeRequests + " outstanding"
                        + " write requests while issuing a read request cxid="
                        + request.cxid + " for session 0x"
                        + Long.toHexString(request.sessionId));
            }
        }

        private void validateRequest(Request request) {
            LOG.info("Got request " + request);

                        if (request.getHdr() != null) {
                long zxid = request.getHdr().getZxid();
                if (!expectedZxid.compareAndSet(zxid, zxid + 1)) {
                    failTest("Write request, expected_zxid="
                             + expectedZxid.get() + "; req_zxid=" + zxid);
                }
            }

                        AtomicInteger sessionCxid = cxidMap.get(request.sessionId);
            if (sessionCxid == null) {
                sessionCxid = new AtomicInteger(request.cxid + 1);
                AtomicInteger existingSessionCxid =
                    cxidMap.putIfAbsent(request.sessionId, sessionCxid);
                if (existingSessionCxid != null) {
                    failTest("Race condition adding cxid=" + request.cxid
                             + " for session 0x"
                             + Long.toHexString(request.sessionId)
                             + " with other_cxid=" + existingSessionCxid.get());
                }
            } else {
                if (!sessionCxid.compareAndSet(
                      request.cxid, request.cxid + 1)) {
                    failTest("Expected_cxid=" + sessionCxid.get()
                             + "; req_cxid=" + request.cxid);
                }
            }
        }

        @Override
        public void shutdown() {
                    }
    }

}
