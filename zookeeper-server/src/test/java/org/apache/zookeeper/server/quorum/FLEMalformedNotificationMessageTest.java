package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FLEMalformedNotificationMessageTest extends ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(FLEMalformedNotificationMessageTest.class);
    private static final byte[] CONFIG_BYTES = "my very invalid config string".getBytes();
    private static final int CONFIG_BYTES_LENGTH = CONFIG_BYTES.length;

    int count;
    HashMap<Long, QuorumServer> peers;
    File tmpdir[];
    int port[];

    QuorumCnxManager mockCnxManager;
    FLETestUtils.LEThread leaderElectionThread;
    QuorumPeer peerRunningLeaderElection;


    @Before
    public void setUp() throws Exception {
        count = 3;

        peers = new HashMap<>(count);
        tmpdir = new File[count];
        port = new int[count];

        LOG.info("FLEMalformedNotificationMessageTest: {}, {}", getTestName(), count);
        for (int i = 0; i < count; i++) {
            int clientport = PortAssignment.unique();
            peers.put((long) i,
                      new QuorumServer(i,
                                       new InetSocketAddress(clientport),
                                       new InetSocketAddress(PortAssignment.unique())));
            tmpdir[i] = ClientBase.createTmpDir();
            port[i] = clientport;
        }

        
        peerRunningLeaderElection = new QuorumPeer(peers, tmpdir[0], tmpdir[0], port[0], 3, 0, 1000, 2, 2);
        peerRunningLeaderElection.startLeaderElection();
        leaderElectionThread = new FLETestUtils.LEThread(peerRunningLeaderElection, 0);
        leaderElectionThread.start();
    }


    @After
    public void tearDown() throws Exception {
        peerRunningLeaderElection.shutdown();
        mockCnxManager.halt();
    }


    @Test
    public void testTooShortPartialNotificationMessage() throws Exception {

        
        startMockServer(1);
        byte requestBytes[] = new byte[12];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.clear();
        requestBuffer.putInt(ServerState.LOOKING.ordinal());           requestBuffer.putLong(0);                                      mockCnxManager.toSend(0L, requestBuffer);

        
        sendValidNotifications(1, 0);
        leaderElectionThread.join(5000);
        if (leaderElectionThread.isAlive()) {
            Assert.fail("Leader election thread didn't join, something went wrong.");
        }
    }


    @Test
    public void testNotificationMessageWithNegativeConfigLength() throws Exception {

        
        startMockServer(1);
        byte requestBytes[] = new byte[48];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.clear();
        requestBuffer.putInt(ServerState.LOOKING.ordinal());           requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putInt(FastLeaderElection.Notification.CURRENTVERSION);           requestBuffer.putInt(-123);                                    mockCnxManager.toSend(0L, requestBuffer);

        
        sendValidNotifications(1, 0);
        leaderElectionThread.join(5000);
        if (leaderElectionThread.isAlive()) {
            Assert.fail("Leader election thread didn't join, something went wrong.");
        }
    }


    @Test
    public void testNotificationMessageWithInvalidConfigLength() throws Exception {

        
        startMockServer(1);
        byte requestBytes[] = new byte[48 + CONFIG_BYTES_LENGTH];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.clear();
        requestBuffer.putInt(ServerState.LOOKING.ordinal());           requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putInt(FastLeaderElection.Notification.CURRENTVERSION);           requestBuffer.putInt(10000);                                   requestBuffer.put(CONFIG_BYTES);                               mockCnxManager.toSend(0L, requestBuffer);

        
        sendValidNotifications(1, 0);
        leaderElectionThread.join(5000);
        if (leaderElectionThread.isAlive()) {
            Assert.fail("Leader election thread didn't join, something went wrong.");
        }
    }


    @Test
    public void testNotificationMessageWithInvalidConfig() throws Exception {

        
        startMockServer(1);
        ByteBuffer requestBuffer = FastLeaderElection.buildMsg(ServerState.LOOKING.ordinal(), 1, 0, 0, 0, CONFIG_BYTES);
        mockCnxManager.toSend(0L, requestBuffer);

        
        sendValidNotifications(1, 0);
        leaderElectionThread.join(5000);
        if (leaderElectionThread.isAlive()) {
            Assert.fail("Leader election thread didn't join, something went wrong.");
        }
    }


    @Test
    public void testNotificationMessageWithBadProtocol() throws Exception {

        
        startMockServer(1);
        byte requestBytes[] = new byte[30];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.clear();
        requestBuffer.putInt(ServerState.LOOKING.ordinal());           requestBuffer.putLong(1);                                      requestBuffer.putLong(0);                                      requestBuffer.putLong(0);                                      requestBuffer.putShort((short) 0);                                      mockCnxManager.toSend(0L, requestBuffer);

        
        sendValidNotifications(1, 0);
        leaderElectionThread.join(5000);
        if (leaderElectionThread.isAlive()) {
            Assert.fail("Leader election thread didn't join, something went wrong.");
        }
    }


    void startMockServer(int sid) throws IOException {
        QuorumPeer peer = new QuorumPeer(peers, tmpdir[sid], tmpdir[sid], port[sid], 3, sid, 1000, 2, 2);
        mockCnxManager = peer.createCnxnManager();
        mockCnxManager.listener.start();
    }


    void sendValidNotifications(int fromSid, int toSid) throws InterruptedException {
        mockCnxManager.toSend((long) toSid, FLETestUtils.createMsg(ServerState.LOOKING.ordinal(), fromSid, 0, 0));
        mockCnxManager.recvQueue.take();
        mockCnxManager.toSend((long) toSid, FLETestUtils.createMsg(ServerState.FOLLOWING.ordinal(), toSid, 0, 0));
    }

}
