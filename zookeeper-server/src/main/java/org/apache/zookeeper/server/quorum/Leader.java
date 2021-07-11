package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);

    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal  extends SyncedLearnerTracker {
        public QuorumPacket packet;
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

        private static final String MAX_CONCURRENT_SNAPSHOTS = "zookeeper.leader.maxConcurrentSnapshots";
    private static final int maxConcurrentSnapshots;
    private static final String MAX_CONCURRENT_SNAPSHOT_TIMEOUT = "zookeeper.leader.maxConcurrentSnapshotTimeout";
    private static final long maxConcurrentSnapshotTimeout;
    static {
        maxConcurrentSnapshots = Integer.getInteger(MAX_CONCURRENT_SNAPSHOTS, 10);
        LOG.info(MAX_CONCURRENT_SNAPSHOTS + " = " + maxConcurrentSnapshots);
        maxConcurrentSnapshotTimeout = Long.getLong(MAX_CONCURRENT_SNAPSHOT_TIMEOUT, 5);
        LOG.info(MAX_CONCURRENT_SNAPSHOT_TIMEOUT + " = " + maxConcurrentSnapshotTimeout);
    }

    private final LearnerSnapshotThrottler learnerSnapshotThrottler;

    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

        protected boolean quorumFormed = false;

        volatile LearnerCnxAcceptor cnxAcceptor = null;

        private final HashSet<LearnerHandler> learners =
        new HashSet<LearnerHandler>();

    private final BufferStats proposalStats;

    public BufferStats getProposalStats() {
        return proposalStats;
    }

    public LearnerSnapshotThrottler createLearnerSnapshotThrottler(
            int maxConcurrentSnapshots, long maxConcurrentSnapshotTimeout) {
        return new LearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

        private final HashSet<LearnerHandler> forwardingFollowers =
        new HashSet<LearnerHandler>();

    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners =
        new HashSet<LearnerHandler>();

    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

        private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs =
        new HashMap<Long,List<LearnerSyncRequest>>();

    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

        final AtomicLong followerCounter = new AtomicLong(-1);

    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);
        }
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }
    }


    public boolean isQuorumSynced(QuorumVerifier qv) {
       HashSet<Long> ids = new HashSet<Long>();
       if (qv.getVotingMembers().containsKey(self.getId()))
           ids.add(self.getId());
       synchronized (forwardingFollowers) {
           for (LearnerHandler learnerHandler: forwardingFollowers){
               if (learnerHandler.synced() && qv.getVotingMembers().containsKey(learnerHandler.getSid())){
                   ids.add(learnerHandler.getSid());
               }
           }
       }
       return qv.containsQuorum(ids);
    }
    
    private final ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new BufferStats();
        try {
            if (self.shouldUsePortUnification() || self.isSslQuorum()) {
                boolean allowInsecureConnection = self.shouldUsePortUnification();
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection, self.getQuorumAddress().getPort());
                } else {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection);
                }
            } else {
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new ServerSocket(self.getQuorumAddress().getPort());
                } else {
                    ss = new ServerSocket();
                }
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk = zk;
        this.learnerSnapshotThrottler = createLearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    final static int DIFF = 13;
    final static int TRUNC = 14;
    final static int SNAP = 15;
    final static int OBSERVERINFO = 16;
    final static int NEWLEADER = 10;
    final static int FOLLOWERINFO = 11;
    final static int UPTODATE = 12;
    public static final int LEADERINFO = 17;
    public static final int ACKEPOCH = 18;
    final static int REQUEST = 1;
    public final static int PROPOSAL = 2;
    final static int ACK = 3;
    final static int COMMIT = 4;
    final static int PING = 5;
    final static int REVALIDATE = 6;
    final static int SYNC = 7;
    final static int INFORM = 8;
    final static int COMMITANDACTIVATE = 9;
    final static int INFORMANDACTIVATE = 19;
    final ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
    private final ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();
    protected final Proposal newLeaderProposal = new Proposal();

    class LearnerCnxAcceptor extends ZooKeeperCriticalThread {
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress(), zk
                    .getZooKeeperServerListener());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    Socket s = null;
                    boolean error = false;
                    try {
                        s = ss.accept();

                                                                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        error = true;
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                                                                                                                stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e){
                        LOG.error("Exception while connecting to quorum learner", e);
                        error = true;
                    } catch (Exception e) {
                        error = true;
                        throw e;
                    } finally {
                                                if (error && s != null && !s.isClosed()) {
                            try {
                                s.close();
                            } catch (IOException e) {
                                LOG.warn("Error closing socket", e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e.getMessage());
                handleException(this.getName(), e);
            }
        }

        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary;

    long epoch = -1;
    boolean waitingForNewEpoch = true;

    boolean allowedToCommit = true;
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick.set(0);
            zk.loadData();

            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

                                    cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();

            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());

            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));

            synchronized(this){
                lastProposed = zk.getZxid();
            }

            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                   null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            QuorumVerifier curQV = self.getQuorumVerifier();
            if (curQV.getVersion() == 0 && curQV.getVersion() == lastSeenQV.getVersion()) {
                                                                                                                                                                                                                                                
                                                                               try {
                   QuorumVerifier newQV = self.configFromString(curQV.toString());
                   newQV.setVersion(zk.getZxid());
                   self.setLastSeenQuorumVerifier(newQV, true);    
               } catch (Exception e) {
                   throw new IOException(e);
               }
            }
            
            newLeaderProposal.addQuorumVerifier(self.getQuorumVerifier());
            if (self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()){
               newLeaderProposal.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }

             waitForEpochAck(self.getId(), leaderStateSummary);
             self.setCurrentEpoch(epoch);    
            
             try {
                 waitForNewLeaderAck(self.getId(), zk.getZxid());
             } catch (InterruptedException e) {
                 shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                         + newLeaderProposal.ackSetsToString() + " ]");
                 HashSet<Long> followerSet = new HashSet<Long>();

                 for(LearnerHandler f : getLearners()) {
                     if (self.getQuorumVerifier().getVotingMembers().containsKey(f.getSid())){
                         followerSet.add(f.getSid());
                     }
                 }    
                 boolean initTicksShouldBeIncreased = true;
                 for (Proposal.QuorumVerifierAcksetPair qvAckset:newLeaderProposal.qvAcksetPairs) {
                     if (!qvAckset.getQuorumVerifier().containsQuorum(followerSet)) {
                         initTicksShouldBeIncreased = false;
                         break;
                     }
                 }                  
                 if (initTicksShouldBeIncreased) {
                     LOG.warn("Enough followers present. "+
                             "Perhaps the initTicks need to be increased.");
                 }
                 return;
             }

             startZkServer();
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }

            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.setZooKeeperServer(zk);
            }

            self.adminServer.setZooKeeperServer(zk);

            boolean tickSkip = true;
            String shutdownMessage = null;

            while (true) {
                synchronized (this) {
                    long start = Time.currentElapsedTime();
                    long cur = start;
                    long end = start + self.tickTime / 2;
                    while (cur < end) {
                        wait(end - cur);
                        cur = Time.currentElapsedTime();
                    }

                    if (!tickSkip) {
                        self.tick.incrementAndGet();
                    }
                    SyncedLearnerTracker syncedAckSet = new SyncedLearnerTracker();
                    syncedAckSet.addQuorumVerifier(self.getQuorumVerifier());
                    if (self.getLastSeenQuorumVerifier() != null
                            && self.getLastSeenQuorumVerifier().getVersion() > self
                                    .getQuorumVerifier().getVersion()) {
                        syncedAckSet.addQuorumVerifier(self
                                .getLastSeenQuorumVerifier());
                    }

                    syncedAckSet.addAck(self.getId());

                    for (LearnerHandler f : getLearners()) {
                        if (f.synced()) {
                            syncedAckSet.addAck(f.getSid());
                        }
                    }

                                        if (!this.isRunning()) {
                                                shutdownMessage = "Unexpected internal error";
                        break;
                    }

                    if (!tickSkip && !syncedAckSet.hasAllQuorums()) {
                                                                        shutdownMessage = "Not sufficient followers synced, only synced with sids: [ "
                                + syncedAckSet.ackSetsToString() + " ]";
                        break;
                    }
                    tickSkip = !tickSkip;
                }
                for (LearnerHandler f : getLearners()) {
                    f.ping();
                }
            }
            if (shutdownMessage != null) {
                shutdown(shutdownMessage);
                            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }

        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }

                self.setZooKeeperServer(null);
        self.adminServer.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        self.closeAllConnections();
                if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    private long getDesignatedLeader(Proposal reconfigProposal, long zxid) {
              Proposal.QuorumVerifierAcksetPair newQVAcksetPair = reconfigProposal.qvAcksetPairs.get(reconfigProposal.qvAcksetPairs.size()-1);        
       
                     if (newQVAcksetPair.getQuorumVerifier().getVotingMembers().containsKey(self.getId()) && 
               newQVAcksetPair.getQuorumVerifier().getVotingMembers().get(self.getId()).addr.equals(self.getQuorumAddress())){  
           return self.getId();
       }
                            HashSet<Long> candidates = new HashSet<Long>(newQVAcksetPair.getAckset());
       candidates.remove(self.getId());        long curCandidate = candidates.iterator().next();
       
                     
       long curZxid = zxid + 1;
       Proposal p = outstandingProposals.get(curZxid);
               
       while (p!=null && !candidates.isEmpty()) {                              
           for (Proposal.QuorumVerifierAcksetPair qvAckset: p.qvAcksetPairs){ 
                              candidates.retainAll(qvAckset.getAckset());
                              if (candidates.isEmpty()) return curCandidate;
                              curCandidate = candidates.iterator().next();
               if (candidates.size() == 1) return curCandidate;
           }      
           curZxid++;
           p = outstandingProposals.get(curZxid);
       }
       
       return curCandidate;
    }

    
    synchronized public boolean tryToCommit(Proposal p, long zxid, SocketAddress followerAddr) {       
                                                        if (outstandingProposals.containsKey(zxid - 1)) return false;
       
                             if (!p.hasAllQuorums()) {
           return false;                 
        }
        
                if (zxid != lastCommitted+1) {    
           LOG.warn("Commiting zxid 0x" + Long.toHexString(zxid)
                    + " from " + followerAddr + " not first!");
            LOG.warn("First is "
                    + (lastCommitted+1));
        }     
        
        outstandingProposals.remove(zxid);
        
        if (p.request != null) {
             toBeApplied.add(p);
        }

        if (p.request == null) {
            LOG.warn("Going to commmit null: " + p);
        } else if (p.request.getHdr().getType() == OpCode.reconfig) {                                   
            LOG.debug("Committing a reconfiguration! " + outstandingProposals.size()); 
                 
                                                            Long designatedLeader = getDesignatedLeader(p, zxid);
            
            QuorumVerifier newQV = p.qvAcksetPairs.get(p.qvAcksetPairs.size()-1).getQuorumVerifier();
       
            self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);

            if (designatedLeader != self.getId()) {
                allowedToCommit = false;
            }
                   
                                                commitAndActivate(zxid, designatedLeader);
            informAndActivate(p, designatedLeader);
                    } else {
            commit(zxid);
            inform(p);
        }
        zk.commitProcessor.commit(p.request);
        if(pendingSyncs.containsKey(zxid)){
            for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                sendSync(r);
            }               
        } 
        
        return  true;   
    }
    
    
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {        
        if (!allowedToCommit) return;                                              if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }
        
        if ((zxid & 0xffffffffL) == 0) {
            
            return;
        }
            
            
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
                        return;
        }
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }
        
        p.addAck(sid);        
        
        
        boolean hasCommitted = tryToCommit(p, zxid, followerAddr);

                                                           
        if (hasCommitted && p.request!=null && p.request.getHdr().getType() == OpCode.reconfig){
               long curZxid = zxid;
           while (allowedToCommit && hasCommitted && p!=null){
               curZxid++;
               p = outstandingProposals.get(curZxid);
               if (p !=null) hasCommitted = tryToCommit(p, curZxid, null);             
           }
        }
    }
    
    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private final RequestProcessor next;

        private final Leader leader;

        
        ToBeAppliedRequestProcessor(RequestProcessor next, Leader leader) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.leader = leader;
            this.next = next;
        }

        
        public void processRequest(Request request) throws RequestProcessorException {
            next.processRequest(request);

                                                            if (request.getHdr() != null) {
                long zxid = request.getHdr().getZxid();
                Iterator<Proposal> iter = leader.toBeApplied.iterator();
                if (iter.hasNext()) {
                    Proposal p = iter.next();
                    if (p.request != null && p.request.zxid == zxid) {
                        iter.remove();
                        return;
                    }
                }
                LOG.error("Committed request not found on toBeApplied: "
                          + request);
            }
        }

        
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {
                f.queuePacket(qp);
            }
        }
    }

    
    void sendObserverPacket(QuorumPacket qp) {
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    long lastCommitted = -1;

    
    public void commit(long zxid) {
        synchronized(this){
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }

        public void commitAndActivate(long zxid, long designatedLeader) {
        synchronized(this){
            lastCommitted = zxid;
        }
        
        byte data[] = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(data);                            
       buffer.putLong(designatedLeader);
       
        QuorumPacket qp = new QuorumPacket(Leader.COMMITANDACTIVATE, zxid, data, null);
        sendPacket(qp);
    }

    
    public void inform(Proposal proposal) {
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid,
                                            proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    
    
    public void informAndActivate(Proposal proposal, long designatedLeader) {
       byte[] proposalData = proposal.packet.getData();
        byte[] data = new byte[proposalData.length + 8];
        ByteBuffer buffer = ByteBuffer.wrap(data);                            
       buffer.putLong(designatedLeader);
       buffer.put(proposalData);
       
        QuorumPacket qp = new QuorumPacket(Leader.INFORMANDACTIVATE, proposal.request.zxid, data, null);
        sendObserverPacket(qp);
    }

    long lastProposed;


    
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }

    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    
    public Proposal propose(Request request) throws XidRolloverException {
        
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }

        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastBufferSize(data.length);
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);

        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;                
        
        synchronized(this) {
           p.addQuorumVerifier(self.getQuorumVerifier());
                   
           if (request.getHdr().getType() == OpCode.reconfig){
               self.setLastSeenQuorumVerifier(request.qv, true);                       
           }
           
           if (self.getQuorumVerifier().getVersion()<self.getLastSeenQuorumVerifier().getVersion()) {
               p.addQuorumVerifier(self.getLastSeenQuorumVerifier());
           }
                   
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();
            outstandingProposals.put(lastProposed, p);
            sendPacket(pp);
        }
        return p;
    }
    
    public LearnerSnapshotThrottler getLearnerSnapshotThrottler() {
        return learnerSnapshotThrottler;
    }

    

    synchronized public void processSync(LearnerSyncRequest r){
        if(outstandingProposals.isEmpty()){
            sendSync(r);
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }

    
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }

    
    synchronized public long startForwarding(LearnerHandler handler,
            long lastSeenZxid) {
                        if (lastProposed > lastSeenZxid) {
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                                                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
                        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid: zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }

        return lastProposed;
    }
        protected final Set<Long> connectingFollowers = new HashSet<Long>();
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized(connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch+1;
            }
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) &&
                                            verifier.containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");
                }
            }
            return epoch;
        }
    }

        protected final Set<Long> electingFollowers = new HashSet<Long>();
        protected boolean electionFinished = false;
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized(electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                if (isParticipant(id)) {
                    electingFollowers.add(id);
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }
    
    
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
              break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    
    private synchronized void startZkServer() {
                lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                + newLeaderProposal.ackSetsToString()
                + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));
        
        
        QuorumVerifier newQV = self.getLastSeenQuorumVerifier();
        
        Long designatedLeader = getDesignatedLeader(newLeaderProposal, zk.getZxid());                                         

        self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);
        if (designatedLeader != self.getId()) {
            allowedToCommit = false;
        }
        
        zk.startup();
        
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.qvAcksetPairs) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            
            newLeaderProposal.addAck(sid);

            if (newLeaderProposal.hasAllQuorums()) {
                quorumFormed = true;
                newLeaderProposal.qvAcksetPairs.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.qvAcksetPairs.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case COMMITANDACTIVATE:
            return "COMMITANDACTIVATE";           
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        case INFORMANDACTIVATE:
            return "INFORMANDACTIVATE";
        default:
            return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getQuorumVerifier().getVotingMembers().containsKey(sid);
    }
}
