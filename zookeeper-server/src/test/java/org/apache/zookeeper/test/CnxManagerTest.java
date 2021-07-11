package org.apache.zookeeper.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.QuorumCnxManager;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.InitialMessage;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CnxManagerTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(FLENewEpochTest.class);
    protected static final int THRESHOLD = 4;

    int count;
    HashMap<Long,QuorumServer> peers;
    File peerTmpdir[];
    int peerQuorumPort[];
    int peerClientPort[];
    @Before
    public void setUp() throws Exception {

        this.count = 3;
        this.peers = new HashMap<Long,QuorumServer>(count);
        peerTmpdir = new File[count];
        peerQuorumPort = new int[count];
        peerClientPort = new int[count];

        for(int i = 0; i < count; i++) {
            peerQuorumPort[i] = PortAssignment.unique();
            peerClientPort[i] = PortAssignment.unique();
            peers.put(Long.valueOf(i),
                new QuorumServer(i,
                    new InetSocketAddress(
                        "127.0.0.1", peerQuorumPort[i]),
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", peerClientPort[i])));
            peerTmpdir[i] = ClientBase.createTmpDir();
        }
    }

    ByteBuffer createMsg(int state, long leader, long zxid, long epoch){
        byte requestBytes[] = new byte[28];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(epoch);

        return requestBuffer;
    }

    class CnxManagerThread extends Thread {

        boolean failed;
        CnxManagerThread(){
            failed = false;
        }

        public void run(){
            try {
                QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[0], peerTmpdir[0], peerClientPort[0], 3, 0, 1000, 2, 2);
                QuorumCnxManager cnxManager = peer.createCnxnManager();
                QuorumCnxManager.Listener listener = cnxManager.listener;
                if(listener != null){
                    listener.start();
                } else {
                    LOG.error("Null listener when initializing cnx manager");
                }

                long sid = 1;
                cnxManager.toSend(sid, createMsg(ServerState.LOOKING.ordinal(), 0, -1, 1));

                Message m = null;
                int numRetries = 1;
                while((m == null) && (numRetries++ <= THRESHOLD)){
                    m = cnxManager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                    if(m == null) cnxManager.connectAll();
                }

                if(numRetries > THRESHOLD){
                    failed = true;
                    return;
                }

                cnxManager.testInitiateConnection(sid);

                m = cnxManager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                if(m == null){
                    failed = true;
                    return;
                }
            } catch (Exception e) {
                LOG.error("Exception while running mock thread", e);
                Assert.fail("Unexpected exception");
            }
        }
    }

    @Test
    public void testCnxManager() throws Exception {
        CnxManagerThread thread = new CnxManagerThread();

        thread.start();

        QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[1], peerTmpdir[1], peerClientPort[1], 3, 1, 1000, 2, 2);
        QuorumCnxManager cnxManager = peer.createCnxnManager();
        QuorumCnxManager.Listener listener = cnxManager.listener;
        if(listener != null){
            listener.start();
        } else {
            LOG.error("Null listener when initializing cnx manager");
        }

        cnxManager.toSend(0L, createMsg(ServerState.LOOKING.ordinal(), 1, -1, 1));

        Message m = null;
        int numRetries = 1;
        while((m == null) && (numRetries++ <= THRESHOLD)){
            m = cnxManager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
            if(m == null) cnxManager.connectAll();
        }

        Assert.assertTrue("Exceeded number of retries", numRetries <= THRESHOLD);

        thread.join(5000);
        if (thread.isAlive()) {
            Assert.fail("Thread didn't join");
        } else {
            if(thread.failed)
                Assert.fail("Did not receive expected message");
        }
        cnxManager.halt();
        assertFalse(cnxManager.listener.isAlive());
    }

    @Test
    public void testCnxManagerTimeout() throws Exception {
        Random rand = new Random();
        byte b = (byte) rand.nextInt();
        int deadPort = PortAssignment.unique();
        String deadAddress = "10.1.1." + b;

        LOG.info("This is the dead address I'm trying: " + deadAddress);

        peers.put(Long.valueOf(2),
                new QuorumServer(2,
                        new InetSocketAddress(deadAddress, deadPort),
                        new InetSocketAddress(deadAddress, PortAssignment.unique()),
                        new InetSocketAddress(deadAddress, PortAssignment.unique())));
        peerTmpdir[2] = ClientBase.createTmpDir();

        QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[1], peerTmpdir[1], peerClientPort[1], 3, 1, 1000, 2, 2);
        QuorumCnxManager cnxManager = peer.createCnxnManager();
        QuorumCnxManager.Listener listener = cnxManager.listener;
        if(listener != null){
            listener.start();
        } else {
            LOG.error("Null listener when initializing cnx manager");
        }

        long begin = Time.currentElapsedTime();
        cnxManager.toSend(2L, createMsg(ServerState.LOOKING.ordinal(), 1, -1, 1));
        long end = Time.currentElapsedTime();

        if((end - begin) > 6000) Assert.fail("Waited more than necessary");
        cnxManager.halt();
        assertFalse(cnxManager.listener.isAlive());
    }

    
    @Test
    public void testCnxManagerSpinLock() throws Exception {
        QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[1], peerTmpdir[1], peerClientPort[1], 3, 1, 1000, 2, 2);
        QuorumCnxManager cnxManager = peer.createCnxnManager();
        QuorumCnxManager.Listener listener = cnxManager.listener;
        if(listener != null){
            listener.start();
        } else {
            LOG.error("Null listener when initializing cnx manager");
        }

        int port = peers.get(peer.getId()).electionAddr.getPort();
        LOG.info("Election port: " + port);

        Thread.sleep(1000);

        SocketChannel sc = SocketChannel.open();
        sc.socket().connect(peers.get(1L).electionAddr, 5000);

        InetSocketAddress otherAddr = peers.get(Long.valueOf(2)).electionAddr;
        DataOutputStream dout = new DataOutputStream(sc.socket().getOutputStream());
        dout.writeLong(QuorumCnxManager.PROTOCOL_VERSION);
        dout.writeLong(2);
        String addr = otherAddr.getHostString()+ ":" + otherAddr.getPort();
        byte[] addr_bytes = addr.getBytes();
        dout.writeInt(addr_bytes.length);
        dout.write(addr_bytes);
        dout.flush();
        

        ByteBuffer msgBuffer = ByteBuffer.wrap(new byte[4]);
        msgBuffer.putInt(-20);
        msgBuffer.position(0);
        sc.write(msgBuffer);

        Thread.sleep(1000);

        try{
            
            for(int i = 0; i < 100; i++){
                msgBuffer.position(0);
                sc.write(msgBuffer);
            }
            Assert.fail("Socket has not been closed");
        } catch (Exception e) {
            LOG.info("Socket has been closed as expected");
        }
        peer.shutdown();
        cnxManager.halt();
        assertFalse(cnxManager.listener.isAlive());
    }

    
    @Test
    public void testCnxManagerListenerThreadConfigurableRetry() throws Exception {
        final Map<Long,QuorumServer> unresolvablePeers = new HashMap<>();
        final long myid = 1L;
        unresolvablePeers.put(myid, new QuorumServer(myid, "unresolvable-domain.org:2182:2183;2181"));
        final QuorumPeer peer = new QuorumPeer(unresolvablePeers,
                                               ClientBase.createTmpDir(),
                                               ClientBase.createTmpDir(),
                                               2181, 3, myid, 1000, 2, 2);
        final QuorumCnxManager cnxManager = peer.createCnxnManager();
        final QuorumCnxManager.Listener listener = cnxManager.listener;
        final AtomicBoolean errorHappend = new AtomicBoolean();
        listener.setSocketBindErrorHandler(() -> errorHappend.set(true));
        listener.start();
                        listener.join(15000);         assertFalse(listener.isAlive());
        Assert.assertTrue(errorHappend.get());
        assertFalse(QuorumPeer.class.getSimpleName() + " not stopped after "
                           + "listener thread death", listener.isAlive());
    }

    
    @Test
    public void testCnxManagerNPE() throws Exception {
                peers.get(2L).type = LearnerType.OBSERVER;
        QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[1], peerTmpdir[1],
                peerClientPort[1], 3, 1, 1000, 2, 2);
        QuorumCnxManager cnxManager = peer.createCnxnManager();
        QuorumCnxManager.Listener listener = cnxManager.listener;
        if (listener != null) {
            listener.start();
        } else {
            LOG.error("Null listener when initializing cnx manager");
        }
        int port = peers.get(peer.getId()).electionAddr.getPort();
        LOG.info("Election port: " + port);

        Thread.sleep(1000);

        SocketChannel sc = SocketChannel.open();
        sc.socket().connect(peers.get(1L).electionAddr, 5000);

        
        byte[] msgBytes = new byte[8];
        ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
        msgBuffer.putLong(2L);
        msgBuffer.position(0);
        sc.write(msgBuffer);

        msgBuffer = ByteBuffer.wrap(new byte[8]);
                msgBuffer.putInt(4);
                msgBuffer.putInt(5);
        msgBuffer.position(0);
        sc.write(msgBuffer);

        Message m = cnxManager.pollRecvQueue(1000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(m);

        peer.shutdown();
        cnxManager.halt();
        assertFalse(cnxManager.listener.isAlive());
    }

    
    @Test
    public void testSocketTimeout() throws Exception {
        QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[1], peerTmpdir[1], peerClientPort[1], 3, 1, 2000, 2, 2);
        QuorumCnxManager cnxManager = peer.createCnxnManager();
        QuorumCnxManager.Listener listener = cnxManager.listener;
        if(listener != null){
            listener.start();
        } else {
            LOG.error("Null listener when initializing cnx manager");
        }
        int port = peers.get(peer.getId()).electionAddr.getPort();
        LOG.info("Election port: " + port);
        Thread.sleep(1000);

        Socket sock = new Socket();
        sock.connect(peers.get(1L).electionAddr, 5000);
        long begin = Time.currentElapsedTime();
                cnxManager.receiveConnection(sock);
        long end = Time.currentElapsedTime();
        if((end - begin) > ((peer.getSyncLimit() * peer.getTickTime()) + 500)) Assert.fail("Waited more than necessary");
        cnxManager.halt();
        assertFalse(cnxManager.listener.isAlive());
    }

    
    @Test
    public void testWorkerThreads() throws Exception {
        ArrayList<QuorumPeer> peerList = new ArrayList<QuorumPeer>();
        try {
            for (int sid = 0; sid < 3; sid++) {
                QuorumPeer peer = new QuorumPeer(peers, peerTmpdir[sid],
                        peerTmpdir[sid], peerClientPort[sid], 3, sid, 1000, 2,
                        2);
                LOG.info("Starting peer {}", peer.getId());
                peer.start();
                peerList.add(sid, peer);
            }
            String failure = verifyThreadCount(peerList, 4);
            Assert.assertNull(failure, failure);
            for (int myid = 0; myid < 3; myid++) {
                for (int i = 0; i < 5; i++) {
                                        QuorumPeer peer = peerList.get(myid);
                    LOG.info("Round {}, halting peer ",
                            new Object[] { i, peer.getId() });
                    peer.shutdown();
                    peerList.remove(myid);
                    failure = verifyThreadCount(peerList, 2);
                    Assert.assertNull(failure, failure);
                                        peer = new QuorumPeer(peers, peerTmpdir[myid],
                            peerTmpdir[myid], peerClientPort[myid], 3, myid,
                            1000, 2, 2);
                    LOG.info("Round {}, restarting peer ",
                            new Object[] { i, peer.getId() });
                    peer.start();
                    peerList.add(myid, peer);
                    failure = verifyThreadCount(peerList, 4);
                    Assert.assertNull(failure, failure);
                }
            }
        } finally {
            for (QuorumPeer quorumPeer : peerList) {
                quorumPeer.shutdown();
            }
        }
    }

    
    public String verifyThreadCount(ArrayList<QuorumPeer> peerList, long ecnt)
        throws InterruptedException
    {
        String failure = null;
        for (int i = 0; i < 480; i++) {
            Thread.sleep(500);

            failure = _verifyThreadCount(peerList, ecnt);
            if (failure == null) {
                return null;
            }
        }
        return failure;
    }
    public String _verifyThreadCount(ArrayList<QuorumPeer> peerList, long ecnt) {
        for (int myid = 0; myid < peerList.size(); myid++) {
            QuorumPeer peer = peerList.get(myid);
            QuorumCnxManager cnxManager = peer.getQuorumCnxManager();
            long cnt = cnxManager.getThreadCount();
            if (cnt != ecnt) {
                return new Date()
                    + " Incorrect number of Worker threads for sid=" + myid
                    + " expected " + ecnt + " found " + cnt;
            }
        }
        return null;
    }

    @Test
    public void testInitialMessage() throws Exception {
        InitialMessage msg;
        ByteArrayOutputStream bos;
        DataInputStream din;
        DataOutputStream dout;
        String hostport;

                try {

                        hostport = "10.0.0.2:3888";
            bos = new ByteArrayOutputStream();
            dout = new DataOutputStream(bos);
            dout.writeLong(5L);             dout.writeInt(hostport.getBytes().length);
            dout.writeBytes(hostport);

                        din = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
            msg = InitialMessage.parse(-65530L, din);
            Assert.fail("bad protocol version accepted");
        } catch (InitialMessage.InitialMessageException ex) {}

                try {

            hostport = createLongString(1048576);
            bos = new ByteArrayOutputStream();
            dout = new DataOutputStream(bos);
            dout.writeLong(5L);             dout.writeInt(hostport.getBytes().length);
            dout.writeBytes(hostport);

            din = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
            msg = InitialMessage.parse(QuorumCnxManager.PROTOCOL_VERSION, din);
            Assert.fail("long message accepted");
        } catch (InitialMessage.InitialMessageException ex) {}

                try {

            hostport = "what's going on here?";
            bos = new ByteArrayOutputStream();
            dout = new DataOutputStream(bos);
            dout.writeLong(5L);             dout.writeInt(hostport.getBytes().length);
            dout.writeBytes(hostport);

            din = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
            msg = InitialMessage.parse(QuorumCnxManager.PROTOCOL_VERSION, din);
            Assert.fail("bad hostport accepted");
        } catch (InitialMessage.InitialMessageException ex) {}

                try {

            hostport = "10.0.0.2:3888";
            bos = new ByteArrayOutputStream();
            dout = new DataOutputStream(bos);
            dout.writeLong(5L);             dout.writeInt(hostport.getBytes().length);
            dout.writeBytes(hostport);

                        din = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
            msg = InitialMessage.parse(QuorumCnxManager.PROTOCOL_VERSION, din);
        } catch (InitialMessage.InitialMessageException ex) {
            Assert.fail(ex.toString());
        }
    }

    @Test
    public void testWildcardAddressRecognition() {
        assertTrue(QuorumCnxManager.InitialMessage.isWildcardAddress("0.0.0.0"));
        assertTrue(QuorumCnxManager.InitialMessage.isWildcardAddress("::"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("::1"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("some.unresolvable.host.com"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("127.0.0.1"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("255.255.255.255"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("1.2.3.4"));
        assertFalse(QuorumCnxManager.InitialMessage.isWildcardAddress("www.google.com"));
    }
    private String createLongString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i=0; i < size; i++) {
            sb.append('x');
        }
        return sb.toString();
    }
}
