package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.net.ssl.SSLSocket;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    
    static final int RECV_CAPACITY = 100;
            static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;

    

    private AtomicLong observerCounter = new AtomicLong(-1);

    
    public static final long PROTOCOL_VERSION = -65536L;

    
    static public final int maxBuffer = 2048;

    

    private int cnxTO = 5000;

    final QuorumPeer self;

    
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections.synchronizedSet(new HashSet<>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    
    public final ArrayBlockingQueue<Message> recvQueue;
    
    private final Object recvQLock = new Object();

    

    volatile boolean shutdown = false;

    
    public final Listener listener;

    
    private AtomicInteger threadCnt = new AtomicInteger(0);

    
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");

    
    static final Supplier<Socket> DEFAULT_SOCKET_FACTORY = () -> new Socket();
    private static Supplier<Socket> SOCKET_FACTORY = DEFAULT_SOCKET_FACTORY;
    static void setSocketFactory(Supplier<Socket> factory) {
        SOCKET_FACTORY = factory;
    }


    static public class Message {
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    
    static public class InitialMessage {
        public Long sid;
        public InetSocketAddress electionAddr;

        InitialMessage(Long sid, InetSocketAddress address) {
            this.sid = sid;
            this.electionAddr = address;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {
            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }
        }

        static public InitialMessage parse(Long protocolVersion, DataInputStream din)
                throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION) {
                throw new InitialMessageException(
                        "Got unrecognized protocol version %s", protocolVersion);
            }

            sid = din.readLong();

            int remaining = din.readInt();
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException(
                        "Unreasonable buffer length: %s", remaining);
            }

            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException(
                        "Read only %s bytes out of %s sent by server %s",
                        num_read, remaining, sid);
            }

            String addr = new String(b);
            String[] host_port;
            try {
                host_port = ConfigUtils.getHostAndPort(addr);
            } catch (ConfigException e) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            if (host_port.length != 2) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            int port;
            try {
                port = Integer.parseInt(host_port[1]);
            } catch (NumberFormatException e) {
                throw new InitialMessageException("Bad port number: %s", host_port[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new InitialMessageException("No port number in: %s", addr);
            }

            return new InitialMessage(sid, isWildcardAddress(host_port[0]) ? null :
                    new InetSocketAddress(host_port[0], port));
        }

        
        public static boolean isWildcardAddress(final String hostname) {
            try {
                return InetAddress.getByName(hostname).isAnyLocalAddress();
            } catch (UnknownHostException e) {
                                return false;
            } catch (SecurityException e) {
                LOG.warn("SecurityException in getByName() for" + hostname);
                return false;
            }
        }

        @Override
        public String toString() {
            return "InitialMessage{sid=" + sid + ", electionAddr=" + electionAddr + '}';
        }
    }

    public QuorumCnxManager(QuorumPeer self,
                            final long mySid,
                            Map<Long,QuorumPeer.QuorumServer> view,
                            QuorumAuthServer authServer,
                            QuorumAuthLearner authLearner,
                            int socketTimeout,
                            boolean listenOnAllIPs,
                            int quorumCnxnThreadsSize,
                            boolean quorumSaslAuthEnabled) {
        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();

        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if(cnxToValue != null){
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;

        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;

        initializeConnectionExecutor(mySid, quorumCnxnThreadsSize);

                listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

                private void initializeConnectionExecutor(final long mySid, final int quorumCnxnThreadsSize) {
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup()
                : Thread.currentThread().getThreadGroup();
        final ThreadFactory daemonThFactory = runnable -> new Thread(group, runnable,
            String.format("QuorumConnectionThread-[myid=%d]-%d", mySid, threadIndex.getAndIncrement()));
        this.connectionExecutor = new ThreadPoolExecutor(3, quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                                                         new SynchronousQueue<>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }


    
    public void testInitiateConnection(long sid) {
        LOG.debug("Opening channel to server " + sid);
        initiateConnection(self.getVotingView().get(sid).electionAddr, sid);
    }

    
    public void initiateConnection(final InetSocketAddress electionAddr, final Long sid) {

        Socket sock = null;
        try {
            LOG.debug("Opening channel to server " + sid);
            if (self.isSslQuorum()) {
                SSLSocket sslSock = self.getX509Util().createSSLSocket();
                setSockOpts(sslSock);
                sslSock.connect(electionAddr, cnxTO);
                sslSock.startHandshake();
                sock = sslSock;
                LOG.info("SSL handshake complete with {} - {} - {}", sslSock.getRemoteSocketAddress(),
                         sslSock.getSession().getProtocol(), sslSock.getSession().getCipherSuite());
            } else {
                sock = SOCKET_FACTORY.get();
                setSockOpts(sock);
                sock.connect(electionAddr, cnxTO);
            }
            LOG.debug("Connected to server " + sid);
        } catch (X509Exception e) {
            LOG.warn("Cannot open secure channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        } catch (UnresolvedAddressException | IOException e) {
            LOG.warn("Cannot open channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        }

        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error("Exception while connecting, id: {}, addr: {}, closing learner connection",
                    new Object[] { sid, sock.getRemoteSocketAddress() }, e);
            closeSocket(sock);
        }
    }

    
    public boolean initiateConnectionAsync(final InetSocketAddress electionAddr, final Long sid) {
        if(!inprogressConnections.add(sid)){
                                    LOG.debug("Connection request to server id: {} is already in progress, so skipping this request",
                    sid);
            return true;
        }
        try {
            connectionExecutor.execute(new QuorumConnectionReqThread(electionAddr, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
                                                inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            return false;
        }
        return true;
    }

    
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final InetSocketAddress electionAddr;
        final Long sid;
        QuorumConnectionReqThread(final InetSocketAddress electionAddr, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.electionAddr = electionAddr;
            this.sid = sid;
        }

        @Override
        public void run() {
            try{
                initiateConnection(electionAddr, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }
    }

    private boolean startConnection(Socket sock, Long sid)
            throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        LOG.debug("startConnection (myId:{} --> sid:{})", self.getId(), sid);
        try {
                                    BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

                                    dout.writeLong(PROTOCOL_VERSION);
            dout.writeLong(self.getId());
            String addr = formatInetAddr(self.getElectionAddress());
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();

            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

                QuorumPeer.QuorumServer qps = self.getVotingView().get(sid);
        if (qps != null) {
                        authLearner.authenticate(sock, qps.hostname);
        }

                if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            closeSocket(sock);
                    } else {
            LOG.debug("Have larger server identifier, so keeping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if(vsw != null)
                vsw.finish();

            senderWorkerMap.put(sid, sw);
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(
                    SEND_CAPACITY));

            sw.start();
            rw.start();

            return true;

        }
        return false;
    }

    
    public void receiveConnection(final Socket sock) {
        DataInputStream din = null;
        try {
            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));

            LOG.debug("Sync handling of connection request received from: {}", sock.getRemoteSocketAddress());
            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    
    public void receiveConnectionAsync(final Socket sock) {
        try {
            LOG.debug("Async handling of connection request received from: {}", sock.getRemoteSocketAddress());
            connectionExecutor.execute(
                    new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {
        private final Socket sock;
        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }
    }

    private void handleConnection(Socket sock, DataInputStream din)
            throws IOException {
        Long sid = null, protocolVersion = null;
        InetSocketAddress electionAddr = null;

        try {
            protocolVersion = din.readLong();
            if (protocolVersion >= 0) {                 sid = protocolVersion;
            } else {
                try {
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    electionAddr = init.electionAddr;
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error("Initial message parsing error!", ex);
                    closeSocket(sock);
                    return;
                }
            }

            if (sid == QuorumPeer.OBSERVER_ID) {
                
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer: " + sid);
            }
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge: {}", e);
            closeSocket(sock);
            return;
        }

                authServer.authenticate(sock, din);
                if (sid < self.getId()) {
            
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            
            LOG.debug("Create new connection to server: {}", sid);
            closeSocket(sock);

            if (electionAddr != null) {
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }
        } else if (sid == self.getId()) {
                        LOG.warn("We got a connection request from a server with our own ID. "
                    + "This should be either a configuration error, or a bug.");
        } else {             SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                vsw.finish();
            }

            senderWorkerMap.put(sid, sw);

            queueSendMap.putIfAbsent(sid,
                    new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));

            sw.start();
            rw.start();
        }
    }

    
    public void toSend(Long sid, ByteBuffer b) {
        
        if (this.mySid == sid) {
             b.position(0);
             addToRecvQueue(new Message(b.duplicate(), sid));
            
        } else {
             
             ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(
                SEND_CAPACITY);
             ArrayBlockingQueue<ByteBuffer> oldq = queueSendMap.putIfAbsent(sid, bq);
             if (oldq != null) {
                 addToSendQueue(oldq, b);
             } else {
                 addToSendQueue(bq, b);
             }
             connectOne(sid);

        }
    }

    
    synchronized private boolean connectOne(long sid, InetSocketAddress electionAddr){
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return true;
        }

                                return initiateConnectionAsync(electionAddr, sid);
    }

    
    synchronized void connectOne(long sid){
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return;
        }
        synchronized (self.QV_LOCK) {
            boolean knownId = false;
                                    self.recreateSocketAddresses(sid);
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastCommittedView", self.getId(), sid);
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr))
                    return;
            }
            if (lastSeenQV != null && lastProposedView.containsKey(sid)
                    && (!knownId || (lastProposedView.get(sid).electionAddr !=
                    lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastProposedView", self.getId(), sid);
                if (connectOne(sid, lastProposedView.get(sid).electionAddr))
                    return;
            }
            if (!knownId) {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
        }
    }


    

    public void connectAll(){
        long sid;
        for(Enumeration<Long> en = queueSendMap.keys();
            en.hasMoreElements();){
            sid = en.nextElement();
            connectOne(sid);
        }
    }


    
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }

        return false;
    }

    
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

                try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();

                if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }

    
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            LOG.debug("Server {} is soft-halting sender towards: {}", self.getId(), sw);
            sw.finish();
        }
    }

    
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(tcpKeepAlive);
        sock.setSoTimeout(self.tickTime * self.syncLimit);
    }

    
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    
    public long getThreadCount() {
        return threadCnt.get();
    }

    
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    
    public class Listener extends ZooKeeperThread {

        private static final String ELECTION_PORT_BIND_RETRY = "zookeeper.electionPortBindRetry";
        private static final int DEFAULT_PORT_BIND_MAX_RETRY = 3;

        private final int portBindMaxRetry;
        private Runnable socketBindErrorHandler = () -> System.exit(ExitCode.UNABLE_TO_BIND_QUORUM_PORT.getValue());
        volatile ServerSocket ss = null;

        public Listener() {
                                    super("ListenerThread");

                                    final Integer maxRetry = Integer.getInteger(ELECTION_PORT_BIND_RETRY,
                                                        DEFAULT_PORT_BIND_MAX_RETRY);
            if (maxRetry >= 0) {
                LOG.info("Election port bind maximum retries is {}",
                         maxRetry == 0 ? "infinite" : maxRetry);
                portBindMaxRetry = maxRetry;
            } else {
                LOG.info("'{}' contains invalid value: {}(must be >= 0). "
                         + "Use default value of {} instead.",
                         ELECTION_PORT_BIND_RETRY, maxRetry, DEFAULT_PORT_BIND_MAX_RETRY);
                portBindMaxRetry = DEFAULT_PORT_BIND_MAX_RETRY;
            }
        }

        
        public void setSocketBindErrorHandler(Runnable errorHandler) {
            this.socketBindErrorHandler = errorHandler;
        }

        
        @Override
        public void run() {
            int numRetries = 0;
            InetSocketAddress addr;
            Socket client = null;
            Exception exitException = null;
            while ((!shutdown) && (portBindMaxRetry == 0 || numRetries < portBindMaxRetry)) {
                LOG.debug("Listener thread started, myId: {}", self.getId());
                try {
                    if (self.shouldUsePortUnification()) {
                        LOG.info("Creating TLS-enabled quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), true);
                    } else if (self.isSslQuorum()) {
                        LOG.info("Creating TLS-only quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), false);
                    } else {
                        ss = new ServerSocket();
                    }

                    ss.setReuseAddress(true);

                    if (self.getQuorumListenOnAllIPs()) {
                        int port = self.getElectionAddress().getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                                                                        self.recreateSocketAddresses(self.getId());
                        addr = self.getElectionAddress();
                    }
                    LOG.info("{} is accepting connections now, my election bind port: {}", QuorumCnxManager.this.mySid, addr.toString());
                    setName(addr.toString());
                    ss.bind(addr);
                    while (!shutdown) {
                        try {
                            client = ss.accept();
                            setSockOpts(client);
                            LOG.info("Received connection request from {}", client.getRemoteSocketAddress());
                                                                                                                                                                        if (quorumSaslAuthEnabled) {
                                receiveConnectionAsync(client);
                            } else {
                                receiveConnection(client);
                            }
                            numRetries = 0;
                        } catch (SocketTimeoutException e) {
                            LOG.warn("The socket is listening for the election accepted "
                                     + "and it timed out unexpectedly, but will retry."
                                     + "see ZOOKEEPER-2836");
                        }
                    }
                } catch (IOException e) {
                    if (shutdown) {
                        break;
                    }
                    LOG.error("Exception while listening", e);
                    exitException = e;
                    numRetries++;
                    try {
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                            "Ignoring exception", ie);
                    }
                    closeSocket(client);
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread after "
                          + numRetries + " errors. "
                          + "I won't be able to participate in leader "
                          + "election any longer: "
                          + formatInetAddr(self.getElectionAddress())
                          + ". Use " + ELECTION_PORT_BIND_RETRY + " property to "
                          + "increase retry count.");
                if (exitException instanceof SocketException) {
                                                                                socketBindErrorHandler.run();
                }
            } else if (ss != null) {
                                try {
                    ss.close();
                } catch (IOException ie) {
                                        LOG.debug("Error closing server socket", ie);
                }
            }
        }

        
        void halt(){
            try{

                LOG.debug("Halt called: Trying to close listeners");
                if(ss != null) {
                    LOG.debug("Closing listener: "
                              + QuorumCnxManager.this.mySid);
                    ss.close();
                }
            } catch (IOException e){
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    
    class SendWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        
        synchronized RecvWorker getRecvWorker(){
            return recvWorker;
        }

        synchronized boolean finish() {
            LOG.debug("Calling SendWorker.finish for {}", sid);

            if(!running){
                
                return running;
            }

            running = false;
            closeSocket(sock);

            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid=" + sid);

            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }

        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || isSendQueueEmpty(bq)) {
                   ByteBuffer b = lastMessageSent.get(sid);
                   if (b != null) {
                       LOG.debug("Attempting to send lastMessage to sid=" + sid);
                       send(b);
                   }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }
            LOG.debug("SendWorker thread started towards {}. myId: {}", sid, QuorumCnxManager.this.mySid);
            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap
                                .get(sid);
                        if (bq != null) {
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("No queue of incoming messages for " +
                                      "server " + sid);
                            break;
                        }

                        if(b != null){
                            lastMessageSent.put(sid, b);
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid
                         + " my id = " + QuorumCnxManager.this.mySid
                         + " error = " + e);
            }
            this.finish();
            LOG.warn("Send worker leaving thread " + " id " + sid + " my id = " + self.getId());
        }
    }

    
    class RecvWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        volatile boolean running = true;
        final DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        
        synchronized boolean finish() {
            LOG.debug("RecvWorker.finish called. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
            if(!running){
                
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                LOG.debug("RecvWorker thread towards {} started. myId: {}", sid, QuorumCnxManager.this.mySid);
                while (running && !shutdown && sock != null) {
                    
                    int length = din.readInt();
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    ByteBuffer message = ByteBuffer.wrap(msgArray);
                    addToRecvQueue(new Message(message.duplicate(), sid));
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = "
                         + QuorumCnxManager.this.mySid + ", error = " , e);
            } finally {
                LOG.warn("Interrupting SendWorker thread from RecvWorker. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
                sw.finish();
                closeSocket(sock);
            }
        }
    }

    
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          ByteBuffer buffer) {
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
                        LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          long timeout, TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    
    public void addToRecvQueue(Message msg) {
        synchronized(recvQLock) {
            if (recvQueue.remainingCapacity() == 0) {
                try {
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                                         LOG.debug("Trying to remove from an empty " +
                         "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                recvQueue.add(msg);
            } catch (IllegalStateException ie) {
                                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    
    public Message pollRecvQueue(long timeout, TimeUnit unit)
       throws InterruptedException {
       return recvQueue.poll(timeout, unit);
    }

    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }
}
