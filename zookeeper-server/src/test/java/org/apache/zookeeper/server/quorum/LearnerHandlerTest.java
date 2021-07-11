package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.TxnLogProposalIterator;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LearnerHandlerTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory
            .getLogger(LearnerHandlerTest.class);

    class MockLearnerHandler extends LearnerHandler {
        boolean threadStarted = false;

        MockLearnerHandler(Socket sock, Leader leader) throws IOException {
            super(sock, new BufferedInputStream(sock.getInputStream()), leader);
        }

        protected void startSendingPackets() {
            threadStarted = true;
        }
    }

    class MockZKDatabase extends ZKDatabase {
        long lastProcessedZxid;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        LinkedList<Proposal> committedLog = new LinkedList<Leader.Proposal>();
        LinkedList<Proposal> txnLog = new LinkedList<Leader.Proposal>();

        public MockZKDatabase(FileTxnSnapLog snapLog) {
            super(snapLog);
        }

        public long getDataTreeLastProcessedZxid() {
            return lastProcessedZxid;
        }

        public long getmaxCommittedLog() {
            if (!committedLog.isEmpty()) {
                return committedLog.getLast().packet.getZxid();
            }
            return 0;
        }

        public long getminCommittedLog() {
            if (!committedLog.isEmpty()) {
                return committedLog.getFirst().packet.getZxid();
            }
            return 0;
        }

        public LinkedList<Proposal> getCommittedLog() {
            return committedLog;
        }

        public ReentrantReadWriteLock getLogLock() {
            return lock;
        }

        public Iterator<Proposal> getProposalsFromTxnLog(long peerZxid,
                long limit) {
            if (peerZxid >= txnLog.peekFirst().packet.getZxid()) {
                return txnLog.iterator();
            } else {
                return (new LinkedList<Proposal>()).iterator();
            }

        }

        public long calculateTxnLogSizeLimit() {
            return 1;
        }
    }

    private MockLearnerHandler learnerHandler;
    private Socket sock;

        private Leader leader;
    private long currentZxid;

        private MockZKDatabase db;

    @Before
    public void setUp() throws Exception {
                leader = mock(Leader.class);
        when(
                leader.startForwarding(ArgumentMatchers.any(LearnerHandler.class),
                        ArgumentMatchers.anyLong())).thenAnswer(new Answer<Long>() {
            public Long answer(InvocationOnMock invocation) {
                currentZxid = invocation.getArgument(1);
                return 0L;
            }
        });

        sock = mock(Socket.class);

        db = new MockZKDatabase(null);
        learnerHandler = new MockLearnerHandler(sock, leader);
    }

    Proposal createProposal(long zxid) {
        Proposal p = new Proposal();
        p.packet = new QuorumPacket();
        p.packet.setZxid(zxid);
        p.packet.setType(Leader.PROPOSAL);
        return p;
    }

    
    public void queuedPacketMatches(long[] zxids) {
        int index = 0;
        for (QuorumPacket qp : learnerHandler.getQueuedPackets()) {
            if (qp.getType() == Leader.PROPOSAL) {
                assertZxidEquals(zxids[index++], qp.getZxid());
            }
        }
    }

    void reset() {
        learnerHandler.getQueuedPackets().clear();
        learnerHandler.threadStarted = false;
        learnerHandler.setFirstPacket(true);
    }

    
    public void assertOpType(int type, long zxid, long currentZxid) {
        Queue<QuorumPacket> packets = learnerHandler.getQueuedPackets();
        assertTrue(packets.size() > 0);
        assertEquals(type, packets.peek().getType());
        assertZxidEquals(zxid, packets.peek().getZxid());
        assertZxidEquals(currentZxid, this.currentZxid);
    }

    void assertZxidEquals(long expected, long value) {
        assertEquals("Expected 0x" + Long.toHexString(expected) + " but was 0x"
                + Long.toHexString(value), expected, value);
    }

    
    @Test
    public void testEmptyCommittedLog() throws Exception {
        long peerZxid;

                peerZxid = 3;
        db.lastProcessedZxid = 1;
        db.committedLog.clear();
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.TRUNC, db.lastProcessedZxid, db.lastProcessedZxid);
        reset();

                peerZxid = 1;
        db.lastProcessedZxid = 1;
        db.committedLog.clear();
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, db.lastProcessedZxid, db.lastProcessedZxid);
        assertEquals(1, learnerHandler.getQueuedPackets().size());
        reset();

                        peerZxid = 0;
        db.setSnapshotSizeFactor(-1);
        db.lastProcessedZxid = 1;
        db.committedLog.clear();
                assertTrue(learnerHandler.syncFollower(peerZxid, db, leader));
        assertEquals(0, learnerHandler.getQueuedPackets().size());
        reset();

    }

    
    @Test
    public void testCommittedLog() throws Exception {
        long peerZxid;

                        db.lastProcessedZxid = 6;
        db.committedLog.add(createProposal(2));
        db.committedLog.add(createProposal(3));
        db.committedLog.add(createProposal(5));

                peerZxid = 4;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.TRUNC, 3, 5);
                assertEquals(3, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 5 });
        reset();

                peerZxid = 2;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, db.getmaxCommittedLog(),
                db.getmaxCommittedLog());
                assertEquals(5, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 3, 5 });
        reset();

                peerZxid = 1;
        db.setSnapshotSizeFactor(-1);
                assertTrue(learnerHandler.syncFollower(peerZxid, db, leader));
        assertEquals(0, learnerHandler.getQueuedPackets().size());
        reset();
    }

    
    @Test
    public void testTxnLog() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(2));
        db.txnLog.add(createProposal(3));
        db.txnLog.add(createProposal(5));
        db.txnLog.add(createProposal(6));
        db.txnLog.add(createProposal(7));
        db.txnLog.add(createProposal(8));
        db.txnLog.add(createProposal(9));

        db.lastProcessedZxid = 9;
        db.committedLog.add(createProposal(6));
        db.committedLog.add(createProposal(7));
        db.committedLog.add(createProposal(8));

                peerZxid = 4;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.TRUNC, 3, db.getmaxCommittedLog());
                assertEquals(9, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 5, 6, 7, 8 });
        reset();

                peerZxid = 3;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, db.getmaxCommittedLog(),
                db.getmaxCommittedLog());
                assertEquals(9, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 5, 6, 7, 8 });
        reset();

    }

    
    @Test
    public void testTxnLogProposalIteratorClosure() throws Exception {
        long peerZxid;

                db = new MockZKDatabase(null) {
            @Override
            public Iterator<Proposal> getProposalsFromTxnLog(long peerZxid,
                    long limit) {
                return TxnLogProposalIterator.EMPTY_ITERATOR;
            }
        };
        db.lastProcessedZxid = 7;
        db.txnLog.add(createProposal(2));
        db.txnLog.add(createProposal(3));

                peerZxid = 4;
        assertTrue("Couldn't identify snapshot transfer!",
                learnerHandler.syncFollower(peerZxid, db, leader));
        reset();
    }

    
    @Test
    public void testTxnLogOnly() throws Exception {
        long peerZxid;

                db.lastProcessedZxid = 7;
        db.txnLog.add(createProposal(2));
        db.txnLog.add(createProposal(3));
        db.txnLog.add(createProposal(5));
        db.txnLog.add(createProposal(6));
        db.txnLog.add(createProposal(7));
        db.txnLog.add(createProposal(8));

                peerZxid = 4;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                        assertOpType(Leader.TRUNC, 3, db.lastProcessedZxid);
                assertEquals(7, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 5, 6, 7 });
        reset();

                peerZxid = 2;
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, db.lastProcessedZxid, db.lastProcessedZxid);
                assertEquals(9, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { 3, 5, 6, 7 });
        reset();

                peerZxid = 1;
        assertTrue(learnerHandler.syncFollower(peerZxid, db, leader));
                assertEquals(0, learnerHandler.getQueuedPackets().size());
        reset();
    }

    long getZxid(long epoch, long counter){
        return ZxidUtils.makeZxid(epoch, counter);
    }

    
    @Test
    public void testTxnLogWithNegativeZxid() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(getZxid(0xf, 2)));
        db.txnLog.add(createProposal(getZxid(0xf, 3)));
        db.txnLog.add(createProposal(getZxid(0xf, 5)));
        db.txnLog.add(createProposal(getZxid(0xf, 6)));
        db.txnLog.add(createProposal(getZxid(0xf, 7)));
        db.txnLog.add(createProposal(getZxid(0xf, 8)));
        db.txnLog.add(createProposal(getZxid(0xf, 9)));

        db.lastProcessedZxid = getZxid(0xf, 9);
        db.committedLog.add(createProposal(getZxid(0xf, 6)));
        db.committedLog.add(createProposal(getZxid(0xf, 7)));
        db.committedLog.add(createProposal(getZxid(0xf, 8)));

                peerZxid = getZxid(0xf, 4);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.TRUNC, getZxid(0xf, 3), db.getmaxCommittedLog());
                assertEquals(9, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(0xf, 5),
                getZxid(0xf, 6), getZxid(0xf, 7), getZxid(0xf, 8) });
        reset();

                peerZxid = getZxid(0xf, 3);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, db.getmaxCommittedLog(),
                db.getmaxCommittedLog());
                assertEquals(9, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(0xf, 5),
                getZxid(0xf, 6), getZxid(0xf, 7), getZxid(0xf, 8) });
        reset();
    }

    
    @Test
    public void testNewEpochZxid() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(getZxid(0, 1)));
        db.txnLog.add(createProposal(getZxid(1, 1)));
        db.txnLog.add(createProposal(getZxid(1, 2)));

                db.lastProcessedZxid = getZxid(2, 0);
        db.committedLog.add(createProposal(getZxid(1, 1)));
        db.committedLog.add(createProposal(getZxid(1, 2)));

                peerZxid = getZxid(0, 0);
                                assertTrue(learnerHandler.syncFollower(peerZxid, db, leader));
        assertEquals(0, learnerHandler.getQueuedPackets().size());
        reset();

                peerZxid = getZxid(1, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(1, 2), getZxid(1, 2));
                assertEquals(5, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(1, 1), getZxid(1, 2)});
        reset();

                peerZxid = getZxid(2, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(2, 0), getZxid(2, 0));
                assertEquals(1, learnerHandler.getQueuedPackets().size());
        reset();

    }

    
    @Test
    public void testNewEpochZxidWithTxnlogOnly() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(getZxid(1, 1)));
        db.txnLog.add(createProposal(getZxid(2, 1)));
        db.txnLog.add(createProposal(getZxid(2, 2)));
        db.txnLog.add(createProposal(getZxid(4, 1)));

                db.lastProcessedZxid = getZxid(6, 0);

                peerZxid = getZxid(3, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(6, 0), getZxid(4, 1));
                assertEquals(3, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(4, 1)});
        reset();

                peerZxid = getZxid(4, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(6, 0), getZxid(4, 1));
                assertEquals(3, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(4, 1)});
        reset();

                peerZxid = getZxid(5, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(6, 0), getZxid(5, 0));
                assertEquals(1, learnerHandler.getQueuedPackets().size());
        reset();

                peerZxid = getZxid(6, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(6, 0), getZxid(6, 0));
                assertEquals(1, learnerHandler.getQueuedPackets().size());
        reset();
    }

    
    @Test
    public void testDuplicatedTxn() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(getZxid(0, 1)));
        db.txnLog.add(createProposal(getZxid(1, 1)));
        db.txnLog.add(createProposal(getZxid(1, 2)));
        db.txnLog.add(createProposal(getZxid(1, 1)));
        db.txnLog.add(createProposal(getZxid(1, 2)));

                db.lastProcessedZxid = getZxid(2, 0);
        db.committedLog.add(createProposal(getZxid(1, 1)));
        db.committedLog.add(createProposal(getZxid(1, 2)));
        db.committedLog.add(createProposal(getZxid(1, 1)));
        db.committedLog.add(createProposal(getZxid(1, 2)));

                peerZxid = getZxid(1, 0);
        assertFalse(learnerHandler.syncFollower(peerZxid, db, leader));
                assertOpType(Leader.DIFF, getZxid(1, 2), getZxid(1, 2));
                assertEquals(5, learnerHandler.getQueuedPackets().size());
        queuedPacketMatches(new long[] { getZxid(1, 1), getZxid(1, 2)});
        reset();

    }

    
    @Test
    public void testCrossEpochTrunc() throws Exception {
        long peerZxid;
        db.txnLog.add(createProposal(getZxid(1, 1)));
        db.txnLog.add(createProposal(getZxid(2, 1)));
        db.txnLog.add(createProposal(getZxid(2, 2)));
        db.txnLog.add(createProposal(getZxid(4, 1)));

                db.lastProcessedZxid = getZxid(6, 0);

                peerZxid = getZxid(3, 1);
        assertTrue(learnerHandler.syncFollower(peerZxid, db, leader));
        assertEquals(0, learnerHandler.getQueuedPackets().size());
        reset();
    }
}
