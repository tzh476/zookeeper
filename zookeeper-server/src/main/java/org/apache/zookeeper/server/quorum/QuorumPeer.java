package org.apache.zookeeper.server.quorum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.sasl.SaslException;

import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.QuorumX509Util;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.auth.SaslQuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.SaslQuorumAuthServer;
import org.apache.zookeeper.server.quorum.auth.NullQuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.NullQuorumAuthServer;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;


public class QuorumPeer extends ZooKeeperThread implements QuorumStats.Provider {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);

    private QuorumBean jmxQuorumBean;
    LocalPeerBean jmxLocalPeerBean;
    private Map<Long, RemotePeerBean> jmxRemotePeerBean;
    LeaderElectionBean jmxLeaderElectionBean;

            private AtomicReference<QuorumCnxManager> qcmRef = new AtomicReference<>();

    QuorumAuthServer authServer;
    QuorumAuthLearner authLearner;

    
    private ZKDatabase zkDb;

    public static final class AddressTuple {
        public final InetSocketAddress quorumAddr;
        public final InetSocketAddress electionAddr;
        public final InetSocketAddress clientAddr;

        public AddressTuple(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this.quorumAddr = quorumAddr;
            this.electionAddr = electionAddr;
            this.clientAddr = clientAddr;
        }
    }

    public static class QuorumServer {
        public InetSocketAddress addr = null;

        public InetSocketAddress electionAddr = null;
        
        public InetSocketAddress clientAddr = null;
        
        public long id;

        public String hostname;
        
        public LearnerType type = LearnerType.PARTICIPANT;
        
        private List<InetSocketAddress> myAddrs;

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this(id, addr, electionAddr, clientAddr, LearnerType.PARTICIPANT);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr) {
            this(id, addr, electionAddr, (InetSocketAddress)null, LearnerType.PARTICIPANT);
        }

                public QuorumServer(long id, InetSocketAddress addr) {
            this(id, addr, (InetSocketAddress)null, (InetSocketAddress)null, LearnerType.PARTICIPANT);
        }
 
        public long getId() {
            return id;
        }

        
        public void recreateSocketAddresses() {
            if (this.addr == null) {
                LOG.warn("Server address has not been initialized");
                return;
            }
            if (this.electionAddr == null) {
                LOG.warn("Election address has not been initialized");
                return;
            }
            String host = this.addr.getHostString();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(host);
            } catch (UnknownHostException ex) {
                LOG.warn("Failed to resolve address: {}", host, ex);
                return;
            }
            LOG.debug("Resolved address for {}: {}", host, address);
            int port = this.addr.getPort();
            this.addr = new InetSocketAddress(address, port);
            port = this.electionAddr.getPort();
            this.electionAddr = new InetSocketAddress(address, port);
        }

        private void setType(String s) throws ConfigException {
            if (s.toLowerCase().equals("observer")) {
               type = LearnerType.OBSERVER;
           } else if (s.toLowerCase().equals("participant")) {
               type = LearnerType.PARTICIPANT;
            } else {
               throw new ConfigException("Unrecognised peertype: " + s);
            }
        }

        private static final String wrongFormat = " does not have the form server_config or server_config;client_config"+
        " where server_config is host:port:port or host:port:port:type and client_config is port or host:port";

        public QuorumServer(long sid, String addressStr) throws ConfigException {
                        this.id = sid;
            String serverClientParts[] = addressStr.split(";");
            String serverParts[] = ConfigUtils.getHostAndPort(serverClientParts[0]);
            if ((serverClientParts.length > 2) || (serverParts.length < 3)
                    || (serverParts.length > 4)) {
                throw new ConfigException(addressStr + wrongFormat);
            }

            if (serverClientParts.length == 2) {
                                String clientParts[] = ConfigUtils.getHostAndPort(serverClientParts[1]);
                if (clientParts.length > 2) {
                    throw new ConfigException(addressStr + wrongFormat);
                }

                                hostname = (clientParts.length == 2) ? clientParts[0] : "0.0.0.0";
                try {
                    clientAddr = new InetSocketAddress(hostname,
                            Integer.parseInt(clientParts[clientParts.length - 1]));
                                    } catch (NumberFormatException e) {
                    throw new ConfigException("Address unresolved: " + hostname + ":" + clientParts[clientParts.length - 1]);
                }
            }

                        try {
                addr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[1]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[1]);
            }
            try {
                electionAddr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[2]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[2]);
            }

            if(addr.getPort() == electionAddr.getPort()) {
                throw new ConfigException(
                        "Client and election port must be different! Please update the configuration file on server." + sid);
            }

            if (serverParts.length == 4) {
                setType(serverParts[3]);
            }

            this.hostname = serverParts[0];
            
            setMyAddrs();
        }

        public QuorumServer(long id, InetSocketAddress addr,
                    InetSocketAddress electionAddr, LearnerType type) {
            this(id, addr, electionAddr, (InetSocketAddress)null, type);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr, InetSocketAddress clientAddr, LearnerType type) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
            this.type = type;
            this.clientAddr = clientAddr;

            setMyAddrs();
        }

        private void setMyAddrs() {
            this.myAddrs = new ArrayList<InetSocketAddress>();
            this.myAddrs.add(this.addr);
            this.myAddrs.add(this.clientAddr);
            this.myAddrs.add(this.electionAddr);
            this.myAddrs = excludedSpecialAddresses(this.myAddrs);
        }

        private static String delimitedHostString(InetSocketAddress addr)
        {
            String host = addr.getHostString();
            if (host.contains(":")) {
                return "[" + host + "]";
            } else {
                return host;
            }
        }

        public String toString(){
            StringWriter sw = new StringWriter();
                        if (addr !=null) {
                sw.append(delimitedHostString(addr));
                sw.append(":");
                sw.append(String.valueOf(addr.getPort()));
            }
            if (electionAddr!=null){
                sw.append(":");
                sw.append(String.valueOf(electionAddr.getPort()));
            }           
            if (type == LearnerType.OBSERVER) sw.append(":observer");
            else if (type == LearnerType.PARTICIPANT) sw.append(":participant");            
            if (clientAddr!=null){
                sw.append(";");
                sw.append(delimitedHostString(clientAddr));
                sw.append(":");
                sw.append(String.valueOf(clientAddr.getPort()));
            }
            return sw.toString();       
        }

        public int hashCode() {
          assert false : "hashCode not designed";
          return 42;         }
        
        private boolean checkAddressesEqual(InetSocketAddress addr1, InetSocketAddress addr2){
            if ((addr1 == null && addr2!=null) ||
                (addr1!=null && addr2==null) ||
                (addr1!=null && addr2!=null && !addr1.equals(addr2))) return false;
            return true;
        }
        
        public boolean equals(Object o){
            if (!(o instanceof QuorumServer)) return false;
            QuorumServer qs = (QuorumServer)o;          
            if ((qs.id != id) || (qs.type != type)) return false;   
            if (!checkAddressesEqual(addr, qs.addr)) return false;
            if (!checkAddressesEqual(electionAddr, qs.electionAddr)) return false;
            if (!checkAddressesEqual(clientAddr, qs.clientAddr)) return false;                    
            return true;
        }

        public void checkAddressDuplicate(QuorumServer s) throws BadArgumentsException {
            List<InetSocketAddress> otherAddrs = new ArrayList<InetSocketAddress>();
            otherAddrs.add(s.addr);
            otherAddrs.add(s.clientAddr);
            otherAddrs.add(s.electionAddr);
            otherAddrs = excludedSpecialAddresses(otherAddrs);

            for (InetSocketAddress my: this.myAddrs) {

                for (InetSocketAddress other: otherAddrs) {
                    if (my.equals(other)) {
                        String error = String.format("%s of server.%d conflicts %s of server.%d", my, this.id, other, s.id);
                        throw new BadArgumentsException(error);
                    }
                }
            }
        }

        private List<InetSocketAddress> excludedSpecialAddresses(List<InetSocketAddress> addrs) {
            List<InetSocketAddress> included = new ArrayList<InetSocketAddress>();
            InetAddress wcAddr = new InetSocketAddress(0).getAddress();

            for (InetSocketAddress addr : addrs) {
                if (addr == null) {
                    continue;
                }
                InetAddress inetaddr = addr.getAddress();

                if (inetaddr == null ||
                    inetaddr.equals(wcAddr) ||                     inetaddr.isLoopbackAddress()) {                     continue;
                }
                included.add(addr);
            }
            return included;
        }
    }


    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }

    
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }

    

    static final long OBSERVER_ID = Long.MAX_VALUE;

    
    public long start_fle, end_fle;     public static final String FLE_TIME_UNIT = "MS";

    
    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    
    public void setLearnerType(LearnerType p) {
        learnerType = p;
    }

    protected synchronized void setConfigFileName(String s) {
        configFilename = s;
    }

    private String configFilename = null;

    public int getQuorumSize(){
        return getVotingView().size();
    }

    

        private QuorumVerifier quorumVerifier;
    
        private QuorumVerifier lastSeenQuorumVerifier = null;

        final Object QV_LOCK = new Object();


    
    private long myid;


    
    public long getId() {
        return myid;
    }

        void setId(long id) {
        this.myid = id;
    }

    private boolean sslQuorum;
    private boolean shouldUsePortUnification;

    public boolean isSslQuorum() {
        return sslQuorum;
    }

    public boolean shouldUsePortUnification() {
        return shouldUsePortUnification;
    }

    private final QuorumX509Util x509Util;

    QuorumX509Util getX509Util() {
        return x509Util;
    }

    
    volatile private Vote currentVote;

    public synchronized Vote getCurrentVote(){
        return currentVote;
    }

    public synchronized void setCurrentVote(Vote v){
        currentVote = v;
    }

    private volatile boolean running = true;

    
    protected int tickTime;

    
    protected boolean localSessionsEnabled = false;

    
    protected boolean localSessionsUpgradingEnabled = true;

    
    protected int minSessionTimeout = -1;

    
    protected int maxSessionTimeout = -1;

    
    protected int initLimit;

    
    protected int syncLimit;
    
    
    protected boolean syncEnabled = true;

    
    protected AtomicInteger tick = new AtomicInteger();

    
    protected boolean quorumListenOnAllIPs = false;

    
    private long electionTimeTaken = -1;

    
    protected boolean quorumSaslEnableAuth;

    
    protected boolean quorumServerSaslAuthRequired;

    
    protected boolean quorumLearnerSaslAuthRequired;

    
    protected String quorumServicePrincipal;

    
    protected String quorumLearnerLoginContext;

    
    protected String quorumServerLoginContext;

        private static final int QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE = 20;
    
    protected int quorumCnxnThreadsSize = QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE;

    
    @Deprecated
    class ResponderThread extends ZooKeeperThread {
        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                byte b[] = new byte[36];
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = "
                                + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        responseBuffer.getInt();                         responseBuffer.putLong(myid);
                        Vote current = getCurrentVote();
                        switch (getPeerState()) {
                        case LOOKING:
                            responseBuffer.putLong(current.getId());
                            responseBuffer.putLong(current.getZxid());
                            break;
                        case LEADING:
                            responseBuffer.putLong(myid);
                            try {
                                long proposed;
                                synchronized(leader) {
                                    proposed = leader.lastProposed;
                                }
                                responseBuffer.putLong(proposed);
                            } catch (NullPointerException npe) {
                                                                                            }
                            break;
                        case FOLLOWING:
                            responseBuffer.putLong(current.getId());
                            try {
                                responseBuffer.putLong(follower.getZxid());
                            } catch (NullPointerException npe) {
                                                                                            }
                            break;
                        case OBSERVING:
                                                                                    break;
                        }
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (RuntimeException e) {
                LOG.warn("Unexpected runtime exception in ResponderThread",e);
            } catch (IOException e) {
                LOG.warn("Unexpected IO exception in ResponderThread",e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }

    private ServerState state = ServerState.LOOKING;
    
    private boolean reconfigFlag = false; 
    public synchronized void setPeerState(ServerState newState){
        state=newState;
    }
    public synchronized void reconfigFlagSet(){
       reconfigFlag = true;
    }
    public synchronized void reconfigFlagClear(){
       reconfigFlag = false;
    }
    public synchronized boolean isReconfigStateChange(){
       return reconfigFlag;
    }
    public synchronized ServerState getPeerState(){
        return state;
    }

    DatagramSocket udpSocket;

    private final AtomicReference<AddressTuple> myAddrs = new AtomicReference<>();

    
    public void recreateSocketAddresses(long id) {
        QuorumVerifier qv = getQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
                if (id == getId()) {
                    setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
                }
            }
        }
        qv = getLastSeenQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
            }
        }
    }

    private AddressTuple getAddrs(){
        AddressTuple addrs = myAddrs.get();
        if (addrs != null) {
            return addrs;
        }
        try {
            synchronized (QV_LOCK) {
                addrs = myAddrs.get();
                while (addrs == null) {
                    QV_LOCK.wait();
                    addrs = myAddrs.get();
                }
                return addrs;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public InetSocketAddress getQuorumAddress(){
        return getAddrs().quorumAddr;
    }
    
    public InetSocketAddress getElectionAddress(){
        return getAddrs().electionAddr;
    }

    public InetSocketAddress getClientAddress(){
        final AddressTuple addrs = myAddrs.get();
        return (addrs == null) ? null : addrs.clientAddr;
    }
    
    private void setAddrs(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr){
        synchronized (QV_LOCK) {
            myAddrs.set(new AddressTuple(quorumAddr, electionAddr, clientAddr));
            QV_LOCK.notifyAll();
        }
    }
    
    private int electionType;

    Election electionAlg;

    ServerCnxnFactory cnxnFactory;
    ServerCnxnFactory secureCnxnFactory;

    private FileTxnSnapLog logFactory = null;

    private final QuorumStats quorumStats;

    AdminServer adminServer;

    public static QuorumPeer testingQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }

    public QuorumPeer() throws SaslException {
        super("QuorumPeer");
        quorumStats = new QuorumStats(this);
        jmxRemotePeerBean = new HashMap<Long, RemotePeerBean>();
        adminServer = AdminServerFactory.createAdminServer();
        x509Util = new QuorumX509Util();
        initialize();
    }

    

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
            File dataLogDir, int electionType,
            long myid, int tickTime, int initLimit, int syncLimit,
            ServerCnxnFactory cnxnFactory) throws IOException {
        this(quorumPeers, dataDir, dataLogDir, electionType, myid, tickTime,
                initLimit, syncLimit, false, cnxnFactory,
                new QuorumMaj(quorumPeers));
    }

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
            File dataLogDir, int electionType,
            long myid, int tickTime, int initLimit, int syncLimit,
            boolean quorumListenOnAllIPs,
            ServerCnxnFactory cnxnFactory,
            QuorumVerifier quorumConfig) throws IOException {
        this();
        this.cnxnFactory = cnxnFactory;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        this.zkDb = new ZKDatabase(this.logFactory);
        if(quorumConfig == null) quorumConfig = new QuorumMaj(quorumPeers);
        setQuorumVerifier(quorumConfig, false);
        adminServer = AdminServerFactory.createAdminServer();
    }

    public void initialize() throws SaslException {
                if (isQuorumSaslAuthEnabled()) {
            Set<String> authzHosts = new HashSet<String>();
            for (QuorumServer qs : getView().values()) {
                authzHosts.add(qs.hostname);
            }
            authServer = new SaslQuorumAuthServer(isQuorumServerSaslAuthRequired(),
                    quorumServerLoginContext, authzHosts);
            authLearner = new SaslQuorumAuthLearner(isQuorumLearnerSaslAuthRequired(),
                    quorumServicePrincipal, quorumLearnerLoginContext);
        } else {
            authServer = new NullQuorumAuthServer();
            authLearner = new NullQuorumAuthLearner();
        }
    }

    QuorumStats quorumStats() {
        return quorumStats;
    }

    @Override
    public synchronized void start() {
        if (!getView().containsKey(myid)) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
         }
        loadDataBase();
        startServerCnxnFactory();
        try {
            adminServer.start();
        } catch (AdminServerException e) {
            LOG.warn("Problem starting AdminServer", e);
            System.out.println(e);
        }
        startLeaderElection();
        super.start();
    }

    private void loadDataBase() {
        try {
            zkDb.loadDataBase();

                        long lastProcessedZxid = zkDb.getDataTree().lastProcessedZxid;
            long epochOfZxid = ZxidUtils.getEpochFromZxid(lastProcessedZxid);
            try {
                currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
            } catch(FileNotFoundException e) {
            	            	            	            	currentEpoch = epochOfZxid;
            	LOG.info(CURRENT_EPOCH_FILENAME
            	        + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
            	        currentEpoch);
            	writeLongToFile(CURRENT_EPOCH_FILENAME, currentEpoch);
            }
            if (epochOfZxid > currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + ", is older than the last zxid, " + lastProcessedZxid);
            }
            try {
                acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
            } catch(FileNotFoundException e) {
            	            	            	            	acceptedEpoch = epochOfZxid;
            	LOG.info(ACCEPTED_EPOCH_FILENAME
            	        + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
            	        acceptedEpoch);
            	writeLongToFile(ACCEPTED_EPOCH_FILENAME, acceptedEpoch);
            }
            if (acceptedEpoch < currentEpoch) {
                throw new IOException("The accepted epoch, " + ZxidUtils.zxidToString(acceptedEpoch) + " is less than the current epoch, " + ZxidUtils.zxidToString(currentEpoch));
            }
        } catch(IOException ie) {
            LOG.error("Unable to load database on disk", ie);
            throw new RuntimeException("Unable to run quorum server ", ie);
        }
    }

    ResponderThread responder;

    synchronized public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }
    synchronized public void startLeaderElection() {
       try {
           if (getPeerState() == ServerState.LOOKING) {
               currentVote = new Vote(myid, getLastLoggedZxid(), getCurrentEpoch());
           }
       } catch(IOException e) {
           RuntimeException re = new RuntimeException(e.getMessage());
           re.setStackTrace(e.getStackTrace());
           throw re;
       }

                             if (electionType == 0) {
            try {
                udpSocket = new DatagramSocket(getQuorumAddress().getPort());
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    
    protected static int countParticipants(Map<Long,QuorumServer> peers) {
      int count = 0;
      for (QuorumServer q : peers.values()) {
          if (q.type == LearnerType.PARTICIPANT) {
              count++;
          }
      }
      return count;
    }

    
    public QuorumPeer(Map<Long,QuorumServer> quorumPeers, File snapDir,
            File logDir, int clientPort, int electionAlg,
            long myid, int tickTime, int initLimit, int syncLimit)
        throws IOException
    {
        this(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                new QuorumMaj(quorumPeers));
    }

    
    public QuorumPeer(Map<Long,QuorumServer> quorumPeers, File snapDir,
            File logDir, int clientPort, int electionAlg,
            long myid, int tickTime, int initLimit, int syncLimit,
            QuorumVerifier quorumConfig)
        throws IOException
    {
        this(quorumPeers, snapDir, logDir, electionAlg,
                myid,tickTime, initLimit,syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                quorumConfig);
    }

    private static InetSocketAddress getClientAddress(Map<Long, QuorumServer> quorumPeers, long myid, int clientPort)
            throws IOException {
        QuorumServer quorumServer = quorumPeers.get(myid);
        if (null == quorumServer) {
            throw new IOException("No QuorumServer correspoding to myid " + myid);
        }
        if (null == quorumServer.clientAddr) {
            return new InetSocketAddress(clientPort);
        }
        if (quorumServer.clientAddr.getPort() != clientPort) {
            throw new IOException("QuorumServer port " + quorumServer.clientAddr.getPort()
                    + " does not match with given port " + clientPort);
        }
        return quorumServer.clientAddr;
    }

    
    public long getLastLoggedZxid() {
        if (!zkDb.isInitialized()) {
            loadDataBase();
        }
        return zkDb.getDataTreeLastProcessedZxid();
    }

    public Follower follower;
    public Leader leader;
    public Observer observer;

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException, X509Exception {
        return new Leader(this, new LeaderZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Observer makeObserver(FileTxnSnapLog logFactory) throws IOException {
        return new Observer(this, new ObserverZooKeeperServer(logFactory, this, this.zkDb));
    }

    @SuppressWarnings("deprecation")
    protected Election createElectionAlgorithm(int electionAlgorithm){
        Election le=null;

                switch (electionAlgorithm) {
        case 0:
            le = new LeaderElection(this);
            break;
        case 1:
            le = new AuthFastLeaderElection(this);
            break;
        case 2:
            le = new AuthFastLeaderElection(this, true);
            break;
        case 3:
            QuorumCnxManager qcm = createCnxnManager();
            QuorumCnxManager oldQcm = qcmRef.getAndSet(qcm);
            if (oldQcm != null) {
                LOG.warn("Clobbering already-set QuorumCnxManager (restarting leader election?)");
                oldQcm.halt();
            }
            QuorumCnxManager.Listener listener = qcm.listener;
            if(listener != null){
                listener.start();
                FastLeaderElection fle = new FastLeaderElection(this, qcm);
                fle.start();
                le = fle;
            } else {
                LOG.error("Null listener when initializing cnx manager");
            }
            break;
        default:
            assert false;
        }
        return le;
    }

    @SuppressWarnings("deprecation")
    protected Election makeLEStrategy(){
        LOG.debug("Initializing leader election protocol...");
        if (getElectionType() == 0) {
            electionAlg = new LeaderElection(this);
        }
        return electionAlg;
    }

    synchronized protected void setLeader(Leader newLeader){
        leader=newLeader;
    }

    synchronized protected void setFollower(Follower newFollower){
        follower=newFollower;
    }

    synchronized protected void setObserver(Observer newObserver){
        observer=newObserver;
    }

    synchronized public ZooKeeperServer getActiveServer(){
        if(leader!=null)
            return leader.zk;
        else if(follower!=null)
            return follower.zk;
        else if (observer != null)
            return observer.zk;
        return null;
    }

    boolean shuttingDownLE = false;
    
    @Override
    public void run() {
        updateThreadName();

        LOG.debug("Starting quorum peer");
        try {
            jmxQuorumBean = new QuorumBean(this);
            MBeanRegistry.getInstance().register(jmxQuorumBean, null);
            for(QuorumServer s: getView().values()){
                ZKMBeanInfo p;
                if (getId() == s.id) {
                    p = jmxLocalPeerBean = new LocalPeerBean(this);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                        jmxLocalPeerBean = null;
                    }
                } else {
                    RemotePeerBean rBean = new RemotePeerBean(this, s);
                    try {
                        MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                        jmxRemotePeerBean.put(s.id, rBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxQuorumBean = null;
        }

        try {
            
            while (running) {
                switch (getPeerState()) {
                case LOOKING:
                    LOG.info("LOOKING");

                    if (Boolean.getBoolean("readonlymode.enabled")) {
                        LOG.info("Attempting to start ReadOnlyZooKeeperServer");

                                                final ReadOnlyZooKeeperServer roZk =
                            new ReadOnlyZooKeeperServer(logFactory, this, this.zkDb);
    
                                                                                                                                                                        Thread roZkMgr = new Thread() {
                            public void run() {
                                try {
                                                                        sleep(Math.max(2000, tickTime));
                                    if (ServerState.LOOKING.equals(getPeerState())) {
                                        roZk.startup();
                                    }
                                } catch (InterruptedException e) {
                                    LOG.info("Interrupted while attempting to start ReadOnlyZooKeeperServer, not started");
                                } catch (Exception e) {
                                    LOG.error("FAILED to start ReadOnlyZooKeeperServer", e);
                                }
                            }
                        };
                        try {
                            roZkMgr.start();
                            reconfigFlagClear();
                            if (shuttingDownLE) {
                                shuttingDownLE = false;
                                startLeaderElection();
                            }
                            setCurrentVote(makeLEStrategy().lookForLeader());
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                            setPeerState(ServerState.LOOKING);
                        } finally {
                                                                                    roZkMgr.interrupt();
                            roZk.shutdown();
                        }
                    } else {
                        try {
                           reconfigFlagClear();
                            if (shuttingDownLE) {
                               shuttingDownLE = false;
                               startLeaderElection();
                               }
                            setCurrentVote(makeLEStrategy().lookForLeader());
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                            setPeerState(ServerState.LOOKING);
                        }                        
                    }
                    break;
                case OBSERVING:
                    try {
                        LOG.info("OBSERVING");
                        setObserver(makeObserver(logFactory));
                        observer.observeLeader();
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception",e );
                    } finally {
                        observer.shutdown();
                        setObserver(null);  
                       updateServerState();
                    }
                    break;
                case FOLLOWING:
                    try {
                       LOG.info("FOLLOWING");
                        setFollower(makeFollower(logFactory));
                        follower.followLeader();
                    } catch (Exception e) {
                       LOG.warn("Unexpected exception",e);
                    } finally {
                       follower.shutdown();
                       setFollower(null);
                       updateServerState();
                    }
                    break;
                case LEADING:
                    LOG.info("LEADING");
                    try {
                        setLeader(makeLeader(logFactory));
                        leader.lead();
                        setLeader(null);
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception",e);
                    } finally {
                        if (leader != null) {
                            leader.shutdown("Forcing shutdown");
                            setLeader(null);
                        }
                        updateServerState();
                    }
                    break;
                }
                start_fle = Time.currentElapsedTime();
            }
        } finally {
            LOG.warn("QuorumPeer main thread exited");
            MBeanRegistry instance = MBeanRegistry.getInstance();
            instance.unregister(jmxQuorumBean);
            instance.unregister(jmxLocalPeerBean);

            for (RemotePeerBean remotePeerBean : jmxRemotePeerBean.values()) {
                instance.unregister(remotePeerBean);
            }

            jmxQuorumBean = null;
            jmxLocalPeerBean = null;
            jmxRemotePeerBean = null;
        }
    }

    private synchronized void updateServerState(){
       if (!reconfigFlag) {
           setPeerState(ServerState.LOOKING);
           LOG.warn("PeerState set to LOOKING");
           return;
       }
       
       if (getId() == getCurrentVote().getId()) {
           setPeerState(ServerState.LEADING);
           LOG.debug("PeerState set to LEADING");
       } else if (getLearnerType() == LearnerType.PARTICIPANT) {
           setPeerState(ServerState.FOLLOWING);
           LOG.debug("PeerState set to FOLLOWING");
       } else if (getLearnerType() == LearnerType.OBSERVER) {
           setPeerState(ServerState.OBSERVING);
           LOG.debug("PeerState set to OBSERVER");
       } else {            setPeerState(ServerState.LOOKING);
           LOG.debug("Shouldn't be here");
       }       
       reconfigFlag = false;   
    }
    
    public void shutdown() {
        running = false;
        x509Util.close();
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        shutdownServerCnxnFactory();
        if(udpSocket != null) {
            udpSocket.close();
        }

        try {
            adminServer.shutdown();
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }

        if(getElectionAlg() != null){
            this.interrupt();
            getElectionAlg().shutdown();
        }
        try {
            zkDb.close();
        } catch (IOException ie) {
            LOG.warn("Error closing logs ", ie);
        }
    }

    
    public Map<Long,QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(getQuorumVerifier().getAllMembers());
    }

    
    public Map<Long,QuorumPeer.QuorumServer> getVotingView() {
        return getQuorumVerifier().getVotingMembers();
    }

    
    public Map<Long,QuorumPeer.QuorumServer> getObservingView() {
       return getQuorumVerifier().getObservingMembers();
    }

    public synchronized Set<Long> getCurrentAndNextConfigVoters() {
        Set<Long> voterIds = new HashSet<Long>(getQuorumVerifier()
                .getVotingMembers().keySet());
        if (getLastSeenQuorumVerifier() != null) {
            voterIds.addAll(getLastSeenQuorumVerifier().getVotingMembers()
                    .keySet());
        }
        return voterIds;
    }
    
    
    public boolean viewContains(Long sid) {
        return this.getView().containsKey(sid);
    }

    
    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                for (LearnerHandler fh : leader.getLearners()) {
                    if (fh.getSocket() != null) {
                        String s = formatInetAddr((InetSocketAddress)fh.getSocket().getRemoteSocketAddress());
                        if (leader.isLearnerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(formatInetAddr((InetSocketAddress)follower.sock.getRemoteSocketAddress()));
            }
        }
        return l.toArray(new String[0]);
    }

    public String getServerState() {
        switch (getPeerState()) {
        case LOOKING:
            return QuorumStats.Provider.LOOKING_STATE;
        case LEADING:
            return QuorumStats.Provider.LEADING_STATE;
        case FOLLOWING:
            return QuorumStats.Provider.FOLLOWING_STATE;
        case OBSERVING:
            return QuorumStats.Provider.OBSERVING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }


    
    public void setMyid(long myid) {
        this.myid = myid;
    }

    
    public int getTickTime() {
        return tickTime;
    }

    
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    
    public int getMaxClientCnxnsPerHost() {
        if (cnxnFactory != null) {
            return cnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureCnxnFactory != null) {
            return secureCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }

    
    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }

    
    public void enableLocalSessions(boolean flag) {
        LOG.info("Local sessions " + (flag ? "enabled" : "disabled"));
        localSessionsEnabled = flag;
    }

    
    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }

    
    public void enableLocalSessionsUpgrading(boolean flag) {
        LOG.info("Local session upgrading " + (flag ? "enabled" : "disabled"));
        localSessionsUpgradingEnabled = flag;
    }

    
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    
    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    
    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    
    public int getInitLimit() {
        return initLimit;
    }

    
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }

    
    public int getTick() {
        return tick.get();
    }

    public QuorumVerifier configFromString(String s) throws IOException, ConfigException{
        Properties props = new Properties();        
        props.load(new StringReader(s));
        return QuorumPeerConfig.parseDynamicConfig(props, electionType, false, false);
    }
    
    
    public QuorumVerifier getQuorumVerifier(){
        synchronized (QV_LOCK) {
            return quorumVerifier;
        }
    }

    
    public QuorumVerifier getLastSeenQuorumVerifier(){
        synchronized (QV_LOCK) {
            return lastSeenQuorumVerifier;
        }
    }
    
    public synchronized void restartLeaderElection(QuorumVerifier qvOLD, QuorumVerifier qvNEW){
        if (qvOLD == null || !qvOLD.equals(qvNEW)) {
            LOG.warn("Restarting Leader Election");
            getElectionAlg().shutdown();
            shuttingDownLE = false;
            startLeaderElection();
        }           
    }

    public String getNextDynamicConfigFilename() {
        if (configFilename == null) {
            LOG.warn("configFilename is null! This should only happen in tests.");
            return null;
        }
        return configFilename + QuorumPeerConfig.nextDynamicConfigFileSuffix;
    }
    
                        private void connectNewPeers(QuorumCnxManager qcm){
        if (quorumVerifier != null && lastSeenQuorumVerifier != null) {
            Map<Long, QuorumServer> committedView = quorumVerifier.getAllMembers();
            for (Entry<Long, QuorumServer> e : lastSeenQuorumVerifier.getAllMembers().entrySet()) {
                if (e.getKey() != getId() && !committedView.containsKey(e.getKey()))
                    qcm.connectOne(e.getKey());
            }
        }
    }

    public void setLastSeenQuorumVerifier(QuorumVerifier qv, boolean writeToDisk){
                                                                QuorumCnxManager qcm = qcmRef.get();
        Object outerLockObject = (qcm != null) ? qcm : QV_LOCK;
        synchronized (outerLockObject) {
            synchronized (QV_LOCK) {
                if (lastSeenQuorumVerifier != null && lastSeenQuorumVerifier.getVersion() > qv.getVersion()) {
                    LOG.error("setLastSeenQuorumVerifier called with stale config " + qv.getVersion() +
                            ". Current version: " + quorumVerifier.getVersion());
                }
                                                if (lastSeenQuorumVerifier != null &&
                        lastSeenQuorumVerifier.getVersion() == qv.getVersion()) {
                    return;
                }
                lastSeenQuorumVerifier = qv;
                if (qcm != null) {
                    connectNewPeers(qcm);
                }

                if (writeToDisk) {
                    try {
                        String fileName = getNextDynamicConfigFilename();
                        if (fileName != null) {
                            QuorumPeerConfig.writeDynamicConfig(fileName, qv, true);
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing next dynamic config file to disk: ", e.getMessage());
                    }
                }
            }
        }
    }
    
    public QuorumVerifier setQuorumVerifier(QuorumVerifier qv, boolean writeToDisk){
        synchronized (QV_LOCK) {
            if ((quorumVerifier != null) && (quorumVerifier.getVersion() >= qv.getVersion())) {
                                                LOG.debug(getId() + " setQuorumVerifier called with known or old config " + qv.getVersion() +
                        ". Current version: " + quorumVerifier.getVersion());
                return quorumVerifier;
            }
            QuorumVerifier prevQV = quorumVerifier;
            quorumVerifier = qv;
            if (lastSeenQuorumVerifier == null || (qv.getVersion() > lastSeenQuorumVerifier.getVersion()))
                lastSeenQuorumVerifier = qv;

            if (writeToDisk) {
                                if (configFilename != null) {
                    try {
                        String dynamicConfigFilename = makeDynamicConfigFilename(
                                qv.getVersion());
                        QuorumPeerConfig.writeDynamicConfig(
                                dynamicConfigFilename, qv, false);
                        QuorumPeerConfig.editStaticConfig(configFilename,
                                dynamicConfigFilename,
                                needEraseClientInfoFromStaticConfig());
                    } catch (IOException e) {
                        LOG.error("Error closing file: ", e.getMessage());
                    }
                } else {
                    LOG.info("writeToDisk == true but configFilename == null");
                }
            }

            if (qv.getVersion() == lastSeenQuorumVerifier.getVersion()) {
                QuorumPeerConfig.deleteFile(getNextDynamicConfigFilename());
            }
            QuorumServer qs = qv.getAllMembers().get(getId());
            if (qs != null) {
                setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
            }
            return prevQV;
        }
    }

    private String makeDynamicConfigFilename(long version) {
        return configFilename + ".dynamic." + Long.toHexString(version);
    }

    private boolean needEraseClientInfoFromStaticConfig() {
        QuorumServer server = quorumVerifier.getAllMembers().get(getId());
        return (server != null && server.clientAddr != null);
    }

    
    public Election getElectionAlg(){
        return electionAlg;
    }

    
    public int getSyncLimit() {
        return syncLimit;
    }

    
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }
    
    
    
    public static final String SYNC_ENABLED = "zookeeper.observer.syncEnabled";
    
    
    public boolean getSyncEnabled() {
        if (System.getProperty(SYNC_ENABLED) != null) {
            LOG.info(SYNC_ENABLED + "=" + Boolean.getBoolean(SYNC_ENABLED));   
            return Boolean.getBoolean(SYNC_ENABLED);
        } else {        
            return syncEnabled;
        }
    }
    
    
    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }

    
    public int getElectionType() {
        return electionType;
    }

    
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }

    public boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }

    public void setQuorumListenOnAllIPs(boolean quorumListenOnAllIPs) {
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
    }

    public void setCnxnFactory(ServerCnxnFactory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }

    public void setSecureCnxnFactory(ServerCnxnFactory secureCnxnFactory) {
        this.secureCnxnFactory = secureCnxnFactory;
    }

    public void setSslQuorum(boolean sslQuorum) {
        if (sslQuorum) {
            LOG.info("Using TLS encrypted quorum communication");
        } else {
            LOG.info("Using insecure (non-TLS) quorum communication");
        }
        this.sslQuorum = sslQuorum;
    }

    public void setUsePortUnification(boolean shouldUsePortUnification) {
        LOG.info("Port unification {}", shouldUsePortUnification ? "enabled" : "disabled");
        this.shouldUsePortUnification = shouldUsePortUnification;
    }

    private void startServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.start();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.start();
        }
    }

    private void shutdownServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
    }

        public void setZooKeeperServer(ZooKeeperServer zks) {
        if (cnxnFactory != null) {
            cnxnFactory.setZooKeeperServer(zks);
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.setZooKeeperServer(zks);
        }
    }

    public void closeAllConnections() {
        if (cnxnFactory != null) {
            cnxnFactory.closeAll();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.closeAll();
        }
    }

    public int getClientPort() {
        if (cnxnFactory != null) {
            return cnxnFactory.getLocalPort();
        }
        return -1;
    }

    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }

    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }

    
    public void setZKDatabase(ZKDatabase database) {
        this.zkDb = database;
    }

    protected ZKDatabase getZkDb() {
        return zkDb;
    }

    public synchronized void initConfigInZKDatabase() {   
        if (zkDb != null) zkDb.initConfigInZKDatabase(getQuorumVerifier());
    }

    public boolean isRunning() {
        return running;
    }

    
    public QuorumCnxManager getQuorumCnxManager() {
        return qcmRef.get();
    }
    private long readLongFromFile(String name) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        try {
            line = br.readLine();
            return Long.parseLong(line);
        } catch(NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    private long acceptedEpoch = -1;
    private long currentEpoch = -1;

    public static final String CURRENT_EPOCH_FILENAME = "currentEpoch";

    public static final String ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

	
        void writeLongToFile(String name, final long value) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        new AtomicFileWritingIdiom(file, new WriterStatement() {
            @Override
            public void write(Writer bw) throws IOException {
                bw.write(Long.toString(value));
            }
        });
    }

    public long getCurrentEpoch() throws IOException {
        if (currentEpoch == -1) {
            currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
        }
        return currentEpoch;
    }

    public long getAcceptedEpoch() throws IOException {
        if (acceptedEpoch == -1) {
            acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
        }
        return acceptedEpoch;
    }

    public void setCurrentEpoch(long e) throws IOException {
        writeLongToFile(CURRENT_EPOCH_FILENAME, e);
        currentEpoch = e;

    }

    public void setAcceptedEpoch(long e) throws IOException {
        writeLongToFile(ACCEPTED_EPOCH_FILENAME, e);
        acceptedEpoch = e;
    }
   
    public boolean processReconfig(QuorumVerifier qv, Long suggestedLeaderId, Long zxid, boolean restartLE) {
       if (!QuorumPeerConfig.isReconfigEnabled()) {
           LOG.debug("Reconfig feature is disabled, skip reconfig processing.");
           return false;
       }

       InetSocketAddress oldClientAddr = getClientAddress();

                     QuorumVerifier prevQV = setQuorumVerifier(qv, true);

                                                               initConfigInZKDatabase();

       if (prevQV.getVersion() < qv.getVersion() && !prevQV.equals(qv)) {
           Map<Long, QuorumServer> newMembers = qv.getAllMembers();
           updateRemotePeerMXBeans(newMembers);
           if (restartLE) restartLeaderElection(prevQV, qv);

           QuorumServer myNewQS = newMembers.get(getId());
           if (myNewQS != null && myNewQS.clientAddr != null
                   && !myNewQS.clientAddr.equals(oldClientAddr)) {
               cnxnFactory.reconfigure(myNewQS.clientAddr);
               updateThreadName();
           }

           boolean roleChange = updateLearnerType(qv);
           boolean leaderChange = false;
           if (suggestedLeaderId != null) {
                              leaderChange = updateVote(suggestedLeaderId, zxid);
           } else {
               long currentLeaderId = getCurrentVote().getId();
               QuorumServer myleaderInCurQV = prevQV.getVotingMembers().get(currentLeaderId);
               QuorumServer myleaderInNewQV = qv.getVotingMembers().get(currentLeaderId);
               leaderChange = (myleaderInCurQV == null || myleaderInCurQV.addr == null || 
                               myleaderInNewQV == null || !myleaderInCurQV.addr.equals(myleaderInNewQV.addr));
                                             reconfigFlagClear();
           }
           
           if (roleChange || leaderChange) {
               return true;
           }
       }
       return false;

   }

    private void updateRemotePeerMXBeans(Map<Long, QuorumServer> newMembers) {
        Set<Long> existingMembers = new HashSet<Long>(newMembers.keySet());
        existingMembers.retainAll(jmxRemotePeerBean.keySet());
        for (Long id : existingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.get(id);
            rBean.setQuorumServer(newMembers.get(id));
        }

        Set<Long> joiningMembers = new HashSet<Long>(newMembers.keySet());
        joiningMembers.removeAll(jmxRemotePeerBean.keySet());
        joiningMembers.remove(getId());         for (Long id : joiningMembers) {
            QuorumServer qs = newMembers.get(id);
            RemotePeerBean rBean = new RemotePeerBean(this, qs);
            try {
                MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                jmxRemotePeerBean.put(qs.id, rBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
        }

        Set<Long> leavingMembers = new HashSet<Long>(jmxRemotePeerBean.keySet());
        leavingMembers.removeAll(newMembers.keySet());
        for (Long id : leavingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.remove(id);
            try {
                MBeanRegistry.getInstance().unregister(rBean);
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
        }
    }

   private boolean updateLearnerType(QuorumVerifier newQV) {        
              if (newQV.getObservingMembers().containsKey(getId())) {
           if (getLearnerType()!=LearnerType.OBSERVER){
               setLearnerType(LearnerType.OBSERVER);
               LOG.info("Becoming an observer");
               reconfigFlagSet();
               return true;
           } else {
               return false;           
           }
       } else if (newQV.getVotingMembers().containsKey(getId())) {
           if (getLearnerType()!=LearnerType.PARTICIPANT){
               setLearnerType(LearnerType.PARTICIPANT);
               LOG.info("Becoming a voting participant");
               reconfigFlagSet();
               return true;
           } else {
               return false;
           }
       }
             if (getLearnerType()!=LearnerType.PARTICIPANT){
          setLearnerType(LearnerType.PARTICIPANT);
          LOG.info("Becoming a non-voting participant");
          reconfigFlagSet();
          return true;
      }
      return false;
   }
   
   private boolean updateVote(long designatedLeader, long zxid){       
       Vote currentVote = getCurrentVote();
       if (currentVote!=null && designatedLeader != currentVote.getId()) {
           setCurrentVote(new Vote(designatedLeader, zxid));
           reconfigFlagSet();
           LOG.warn("Suggested leader: " + designatedLeader);
           return true;
       }
       return false;
   }
 
    
    protected void updateElectionVote(long newEpoch) {
        Vote currentVote = getCurrentVote();
        if (currentVote != null) {
            setCurrentVote(new Vote(currentVote.getId(),
                currentVote.getZxid(),
                currentVote.getElectionEpoch(),
                newEpoch,
                currentVote.getState()));
        }
    }

    private void updateThreadName() {
        String plain = cnxnFactory != null ?
                cnxnFactory.getLocalAddress() != null ?
                        formatInetAddr(cnxnFactory.getLocalAddress()) : "disabled" : "disabled";
        String secure = secureCnxnFactory != null ? formatInetAddr(secureCnxnFactory.getLocalAddress()) : "disabled";
        setName(String.format("QuorumPeer[myid=%d](plain=%s)(secure=%s)", getId(), plain, secure));
    }

    
    void setElectionTimeTaken(long electionTimeTaken) {
        this.electionTimeTaken = electionTimeTaken;
    }

    
    long getElectionTimeTaken() {
        return electionTimeTaken;
    }

    void setQuorumServerSaslRequired(boolean serverSaslRequired) {
        quorumServerSaslAuthRequired = serverSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED,
                serverSaslRequired);
    }

    void setQuorumLearnerSaslRequired(boolean learnerSaslRequired) {
        quorumLearnerSaslAuthRequired = learnerSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED,
                learnerSaslRequired);
    }

    void setQuorumSaslEnabled(boolean enableAuth) {
        quorumSaslEnableAuth = enableAuth;
        if (!quorumSaslEnableAuth) {
            LOG.info("QuorumPeer communication is not secured! (SASL auth disabled)");
        } else {
            LOG.info("{} set to {}",
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED, enableAuth);
        }
    }

    void setQuorumServicePrincipal(String servicePrincipal) {
        quorumServicePrincipal = servicePrincipal;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL,
                quorumServicePrincipal);
    }

    void setQuorumLearnerLoginContext(String learnerContext) {
        quorumLearnerLoginContext = learnerContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT,
                quorumLearnerLoginContext);
    }

    void setQuorumServerLoginContext(String serverContext) {
        quorumServerLoginContext = serverContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT,
                quorumServerLoginContext);
    }

    void setQuorumCnxnThreadsSize(int qCnxnThreadsSize) {
        if (qCnxnThreadsSize > QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE) {
            quorumCnxnThreadsSize = qCnxnThreadsSize;
        }
        LOG.info("quorum.cnxn.threads.size set to {}", quorumCnxnThreadsSize);
    }

    boolean isQuorumSaslAuthEnabled() {
        return quorumSaslEnableAuth;
    }

    private boolean isQuorumServerSaslAuthRequired() {
        return quorumServerSaslAuthRequired;
    }

    private boolean isQuorumLearnerSaslAuthRequired() {
        return quorumLearnerSaslAuthRequired;
    }

    public QuorumCnxManager createCnxnManager() {
        return new QuorumCnxManager(this,
                this.getId(),
                this.getView(),
                this.authServer,
                this.authLearner,
                this.tickTime * this.syncLimit,
                this.getQuorumListenOnAllIPs(),
                this.quorumCnxnThreadsSize,
                this.isQuorumSaslAuthEnabled());
    }

    boolean isLeader(long id) {
        Vote vote = getCurrentVote();
        return vote != null && id == vote.getId();
    }
}
