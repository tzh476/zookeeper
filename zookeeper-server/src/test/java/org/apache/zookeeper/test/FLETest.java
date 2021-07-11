package org.apache.zookeeper.test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.server.quorum.FastLeaderElection;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FLETest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(FLETest.class);
    private final int MAX_LOOP_COUNTER = 300;
    private FLETest.LEThread leThread;

    static class TestVote {
        TestVote(int id, long leader) {
            this.leader = leader;
        }

        long leader;
    }

    int countVotes(HashSet<TestVote> hs, long id) {
        int counter = 0;
        for(TestVote v : hs){
            if(v.leader == id) counter++;
        }

        return counter;
    }

    int count;
    HashMap<Long,QuorumServer> peers;
    ArrayList<LEThread> threads;
    HashMap<Integer, HashSet<TestVote> > voteMap;
    HashMap<Long, LEThread> quora;
    File tmpdir[];
    int port[];
    int successCount;

    volatile Vote votes[];
    volatile long leader = -1;
        Random rand = new Random();
    Set<Long> joinedThreads;
    
    @Before
    public void setUp() throws Exception {
        count = 7;

        peers = new HashMap<Long,QuorumServer>(count);
        threads = new ArrayList<LEThread>(count);
        voteMap = new HashMap<Integer, HashSet<TestVote> >();
        votes = new Vote[count];
        tmpdir = new File[count];
        port = new int[count];
        successCount = 0;
        joinedThreads = new HashSet<Long>();
    }

    @After
    public void tearDown() throws Exception {
        for (int i = 0; i < threads.size(); i++) {
            leThread = threads.get(i);
            QuorumBase.shutdown(leThread.peer);
        }
    }

    
    
    class LEThread extends Thread {
        FLETest self;
        int i;
        QuorumPeer peer;
        int totalRounds;
        ConcurrentHashMap<Long, HashSet<Integer> > quora;

        LEThread(FLETest self, QuorumPeer peer, int i, int rounds, ConcurrentHashMap<Long, HashSet<Integer> > quora) {
            this.self = self;
            this.i = i;
            this.peer = peer;
            this.totalRounds = rounds;
            this.quora = quora;
            
            LOG.info("Constructor: " + getName());
        }
        
        public void run() {
            try {
                Vote v = null;
                while(true) {
                    
                    
                    peer.setPeerState(ServerState.LOOKING);
                    LOG.info("Going to call leader election again.");
                    v = peer.getElectionAlg().lookForLeader();
                    if(v == null){
                        LOG.info("Thread " + i + " got a null vote");
                        break;
                    }

                    
                    peer.setCurrentVote(v);

                    LOG.info("Finished election: " + i + ", " + v.getId());
                    votes[i] = v;

                    
                    int lc = (int) ((FastLeaderElection) peer.getElectionAlg()).getLogicalClock();
                    
                    
                    if (v.getId() == i) {
                        LOG.info("I'm the leader: " + i);
                        if (lc < this.totalRounds) {
                            LOG.info("Leader " + i + " dying");
                            FastLeaderElection election =
                                (FastLeaderElection) peer.getElectionAlg();
                            election.shutdown();
                                                        Assert.assertEquals(-1, election.getVote().getId());
                            LOG.info("Leader " + i + " dead");
                            
                            break;
                        } 
                    }
                    
                    
                    if (lc >= this.totalRounds) {
                        
                        if(quora.get(v.getId()) == null) quora.put(v.getId(), new HashSet<Integer>());
                        quora.get(v.getId()).add(i);
                        
                        
                        if(waitForQuorum(v.getId())){   
                            synchronized(self){
                                
                                
                                if(v.getId() == i){
                                    Assert.assertTrue("Wrong state" + peer.getPeerState(), 
                                                                    peer.getPeerState() == ServerState.LEADING);
                                    leader = i;
                                } else {
                                    Assert.assertTrue("Wrong state" + peer.getPeerState(), 
                                                                    peer.getPeerState() == ServerState.FOLLOWING);
                                }
                                
                                
                                successCount++;
                                joinedThreads.add((long)i);
                                self.notify();
                            }
                        
                            
                            break;
                        } else {
                            quora.get(v.getId()).remove(i);
                        }
                    } 
                    
                    
                    Thread.sleep(100);
                    
                }
                LOG.debug("Thread " + i + " votes " + v);
            } catch (InterruptedException e) {
                Assert.fail(e.toString());
            }
        }
        
        
        boolean waitForQuorum(long id)
        throws InterruptedException {
            int loopCounter = 0;
            while((quora.get(id).size() <= count/2) && (loopCounter < MAX_LOOP_COUNTER)){
                Thread.sleep(100);
                loopCounter++;
            }
            
            if((loopCounter >= MAX_LOOP_COUNTER) && (quora.get(id).size() <= count/2)){
                return false;
            } else {
                return true;
            }
        }
        
    }

    
    
    @Test
    public void testSingleElection() throws Exception {
        try{
            runElection(1);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }
    
    
    @Test
    public void testDoubleElection() throws Exception {
        try{
            runElection(2);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }
    
    @Test
    public void testTripleElection() throws Exception {
        try{
            runElection(3);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    
    private void runElection(int rounds) throws Exception {
        ConcurrentHashMap<Long, HashSet<Integer> > quora = 
            new ConcurrentHashMap<Long, HashSet<Integer> >();

        LOG.info("TestLE: " + getTestName()+ ", " + count);

        
        for(int i = 0; i < count; i++) {
            port[i] = PortAssignment.unique();
            peers.put(Long.valueOf(i),
                new QuorumServer(i,
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", port[i])));
            tmpdir[i] = ClientBase.createTmpDir();           
        }

        
        for(int i = 0; i < count; i++) {
            QuorumPeer peer = new QuorumPeer(peers, tmpdir[i], tmpdir[i],
                    port[i], 3, i, 1000, 2, 2);
            peer.startLeaderElection();
            LEThread thread = new LEThread(this, peer, i, rounds, quora);
            thread.start();
            threads.add(thread);
        }
        LOG.info("Started threads " + getTestName());

        int waitCounter = 0;
        synchronized(this){
            while(((successCount <= count/2) || (leader == -1))
                && (waitCounter < MAX_LOOP_COUNTER))
            {
                this.wait(200);
                waitCounter++;
            }
        }
        LOG.info("Success count: " + successCount);

        
       for (int i = 0; i < threads.size(); i++) {
            if (threads.get(i).isAlive()) {
                LOG.info("Threads didn't join: " + i);
            }
        }

       
       if(successCount <= count/2){
           Assert.fail("Fewer than a a majority has joined");
       }

       
       if(!joinedThreads.contains(leader)){
           Assert.fail("Leader hasn't joined: " + leader);
       }
    }
    
    
    
    static class VerifyState extends Thread {
        volatile private boolean success = false;
        private QuorumPeer peer;
        public VerifyState(QuorumPeer peer) {
            this.peer = peer;
        }
        public void run() {
            setName("VerifyState-" + peer.getId());
            while (true) {
                if(peer.getPeerState() == ServerState.FOLLOWING) {
                    LOG.info("I am following");
                    success = true;
                    break;
                } else if (peer.getPeerState() == ServerState.LEADING) {
                    LOG.info("I am leading");
                    success = false;
                    break;
                }
                try {
                    Thread.sleep(250);
                } catch (Exception e) {
                    LOG.warn("Sleep failed ", e);
                }
            }
        }
        public boolean isSuccess() {
            return success;
        }
    }

    
    @Test
    public void testJoin() throws Exception {
        int sid;
        QuorumPeer peer;
        int waitTime = 10 * 1000;
        ArrayList<QuorumPeer> peerList = new ArrayList<QuorumPeer>();
        for(sid = 0; sid < 3; sid++) {
            port[sid] = PortAssignment.unique();
            peers.put(Long.valueOf(sid),
                new QuorumServer(sid,
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", port[sid])));
            tmpdir[sid] = ClientBase.createTmpDir();          
        }
                for (sid = 0; sid < 2; sid++) {
            peer = new QuorumPeer(peers, tmpdir[sid], tmpdir[sid],
                                             port[sid], 3, sid, 2000, 2, 2);
            LOG.info("Starting peer " + peer.getId());
            peer.start();
            peerList.add(sid, peer);
        }
        peer = peerList.get(0);
        VerifyState v1 = new VerifyState(peerList.get(0));
        v1.start();
        v1.join(waitTime);
        Assert.assertFalse("Unable to form cluster in " +
            waitTime + " ms",
            !v1.isSuccess());
                peer = new QuorumPeer(peers, tmpdir[sid], tmpdir[sid],
                 port[sid], 3, sid, 2000, 2, 2);
        LOG.info("Starting peer " + peer.getId());
        peer.start();
        peerList.add(sid, peer);
        v1 = new VerifyState(peer);
        v1.start();
        v1.join(waitTime);
        if (v1.isAlive()) {
               Assert.fail("Peer " + peer.getId() + " failed to join the cluster " +
                "within " + waitTime + " ms");
        } else if (!v1.isSuccess()) {
               Assert.fail("Incorrect LEADING state for peer " + peer.getId());
        }
                for (int id = 0; id < 3; id++) {
            peer = peerList.get(id);
            if (peer != null) {
                peer.shutdown();
            }
        }
    }

    
    @Test
    public void testJoinInconsistentEnsemble() throws Exception {
        int sid;
        QuorumPeer peer;
        int waitTime = 10 * 1000;
        ArrayList<QuorumPeer> peerList = new ArrayList<QuorumPeer>();
        for(sid = 0; sid < 3; sid++) {
            peers.put(Long.valueOf(sid),
                new QuorumServer(sid,
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique()),
                    new InetSocketAddress(
                        "127.0.0.1", PortAssignment.unique())));
            tmpdir[sid] = ClientBase.createTmpDir();
            port[sid] = PortAssignment.unique();
        }
                for (sid = 0; sid < 2; sid++) {
            peer = new QuorumPeer(peers, tmpdir[sid], tmpdir[sid],
                                             port[sid], 3, sid, 2000, 2, 2);
            LOG.info("Starting peer " + peer.getId());
            peer.start();
            peerList.add(sid, peer);
        }
        peer = peerList.get(0);
        VerifyState v1 = new VerifyState(peerList.get(0));
        v1.start();
        v1.join(waitTime);
        Assert.assertFalse("Unable to form cluster in " +
            waitTime + " ms",
            !v1.isSuccess());
                long leaderSid = peer.getCurrentVote().getId();
        long zxid = peer.getCurrentVote().getZxid();
        long electionEpoch = peer.getCurrentVote().getElectionEpoch();
        ServerState state = peer.getCurrentVote().getState();
        long peerEpoch = peer.getCurrentVote().getPeerEpoch();
        Vote newVote = new Vote(leaderSid, zxid+100, electionEpoch+100, peerEpoch, state);
        peer.setCurrentVote(newVote);
                peer = new QuorumPeer(peers, tmpdir[2], tmpdir[2],
                 port[2], 3, 2, 2000, 2, 2);
        LOG.info("Starting peer " + peer.getId());
        peer.start();
        peerList.add(sid, peer);
        v1 = new VerifyState(peer);
        v1.start();
        v1.join(waitTime);
        if (v1.isAlive()) {
               Assert.fail("Peer " + peer.getId() + " failed to join the cluster " +
                "within " + waitTime + " ms");
        }
                for (int id = 0; id < 3; id++) {
            peer = peerList.get(id);
            if (peer != null) {
                peer.shutdown();
            }
        }
    }

    @Test
    public void testElectionTimeUnit() throws Exception {
        Assert.assertEquals("MS", QuorumPeer.FLE_TIME_UNIT);
    }
}
