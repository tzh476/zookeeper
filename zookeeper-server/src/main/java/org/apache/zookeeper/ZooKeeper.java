package org.apache.zookeeper;

import org.apache.jute.Record;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoWatcherException;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckWatchesRequest;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReconfigRequest;
import org.apache.zookeeper.proto.RemoveWatchesRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.EphemeralType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;



@SuppressWarnings("try")
@InterfaceAudience.Public
public class ZooKeeper implements AutoCloseable {

    
    @Deprecated
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
    
    
    @Deprecated
    public static final String SECURE_CLIENT = "zookeeper.client.secure";

    protected final ClientCnxn cnxn;
    private static final Logger LOG;
    static {
                LOG = LoggerFactory.getLogger(ZooKeeper.class);
        Environment.logEnv("Client environment:", LOG);
    }

    protected final HostProvider hostProvider;

    
    public void updateServerList(String connectString) throws IOException {
        ConnectStringParser connectStringParser = new ConnectStringParser(connectString);
        Collection<InetSocketAddress> serverAddresses = connectStringParser.getServerAddresses();

        ClientCnxnSocket clientCnxnSocket = cnxn.sendThread.getClientCnxnSocket();
        InetSocketAddress currentHost = (InetSocketAddress) clientCnxnSocket.getRemoteSocketAddress();

        boolean reconfigMode = hostProvider.updateServerList(serverAddresses, currentHost);

                        if (reconfigMode) clientCnxnSocket.testableCloseSocket();
    }

    public ZooKeeperSaslClient getSaslClient() {
        return cnxn.zooKeeperSaslClient;
    }

    protected final ZKWatchManager watchManager;

    private final ZKClientConfig clientConfig;

    public ZKClientConfig getClientConfig() {
        return clientConfig;
    }

    protected List<String> getDataWatches() {
        synchronized(watchManager.dataWatches) {
            List<String> rc = new ArrayList<String>(watchManager.dataWatches.keySet());
            return rc;
        }
    }
    protected List<String> getExistWatches() {
        synchronized(watchManager.existWatches) {
            List<String> rc =  new ArrayList<String>(watchManager.existWatches.keySet());
            return rc;
        }
    }
    protected List<String> getChildWatches() {
        synchronized(watchManager.childWatches) {
            List<String> rc = new ArrayList<String>(watchManager.childWatches.keySet());
            return rc;
        }
    }

    
    static class ZKWatchManager implements ClientWatchManager {
        private final Map<String, Set<Watcher>> dataWatches =
            new HashMap<String, Set<Watcher>>();
        private final Map<String, Set<Watcher>> existWatches =
            new HashMap<String, Set<Watcher>>();
        private final Map<String, Set<Watcher>> childWatches =
            new HashMap<String, Set<Watcher>>();
        private boolean disableAutoWatchReset;

        ZKWatchManager(boolean disableAutoWatchReset) {
            this.disableAutoWatchReset = disableAutoWatchReset;
        }

        protected volatile Watcher defaultWatcher;

        final private void addTo(Set<Watcher> from, Set<Watcher> to) {
            if (from != null) {
                to.addAll(from);
            }
        }

        public Map<EventType, Set<Watcher>> removeWatcher(String clientPath,
                Watcher watcher, WatcherType watcherType, boolean local, int rc)
                throws KeeperException {
                                    containsWatcher(clientPath, watcher, watcherType);

            Map<EventType, Set<Watcher>> removedWatchers = new HashMap<EventType, Set<Watcher>>();
            HashSet<Watcher> childWatchersToRem = new HashSet<Watcher>();
            removedWatchers
                    .put(EventType.ChildWatchRemoved, childWatchersToRem);
            HashSet<Watcher> dataWatchersToRem = new HashSet<Watcher>();
            removedWatchers.put(EventType.DataWatchRemoved, dataWatchersToRem);
            boolean removedWatcher = false;
            switch (watcherType) {
            case Children: {
                synchronized (childWatches) {
                    removedWatcher = removeWatches(childWatches, watcher,
                            clientPath, local, rc, childWatchersToRem);
                }
                break;
            }
            case Data: {
                synchronized (dataWatches) {
                    removedWatcher = removeWatches(dataWatches, watcher,
                            clientPath, local, rc, dataWatchersToRem);
                }

                synchronized (existWatches) {
                    boolean removedDataWatcher = removeWatches(existWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
                break;
            }
            case Any: {
                synchronized (childWatches) {
                    removedWatcher = removeWatches(childWatches, watcher,
                            clientPath, local, rc, childWatchersToRem);
                }

                synchronized (dataWatches) {
                    boolean removedDataWatcher = removeWatches(dataWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
                synchronized (existWatches) {
                    boolean removedDataWatcher = removeWatches(existWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
            }
            }
                        if (!removedWatcher) {
                throw new KeeperException.NoWatcherException(clientPath);
            }
            return removedWatchers;
        }

        private boolean contains(String path, Watcher watcherObj,
                Map<String, Set<Watcher>> pathVsWatchers) {
            boolean watcherExists = true;
            if (pathVsWatchers == null || pathVsWatchers.size() == 0) {
                watcherExists = false;
            } else {
                Set<Watcher> watchers = pathVsWatchers.get(path);
                if (watchers == null) {
                    watcherExists = false;
                } else if (watcherObj == null) {
                    watcherExists = watchers.size() > 0;
                } else {
                    watcherExists = watchers.contains(watcherObj);
                }
            }
            return watcherExists;
        }

        
        void containsWatcher(String path, Watcher watcher,
                WatcherType watcherType) throws NoWatcherException{
            boolean containsWatcher = false;
            switch (watcherType) {
            case Children: {
                synchronized (childWatches) {
                    containsWatcher = contains(path, watcher, childWatches);
                }
                break;
            }
            case Data: {
                synchronized (dataWatches) {
                    containsWatcher = contains(path, watcher, dataWatches);
                }

                synchronized (existWatches) {
                    boolean contains_temp = contains(path, watcher,
                            existWatches);
                    containsWatcher |= contains_temp;
                }
                break;
            }
            case Any: {
                synchronized (childWatches) {
                    containsWatcher = contains(path, watcher, childWatches);
                }

                synchronized (dataWatches) {
                    boolean contains_temp = contains(path, watcher, dataWatches);
                    containsWatcher |= contains_temp;
                }
                synchronized (existWatches) {
                    boolean contains_temp = contains(path, watcher,
                            existWatches);
                    containsWatcher |= contains_temp;
                }
            }
            }
                        if (!containsWatcher) {
                throw new KeeperException.NoWatcherException(path);
            }
        }

        protected boolean removeWatches(Map<String, Set<Watcher>> pathVsWatcher,
                Watcher watcher, String path, boolean local, int rc,
                Set<Watcher> removedWatchers) throws KeeperException {
            if (!local && rc != Code.OK.intValue()) {
                throw KeeperException
                        .create(KeeperException.Code.get(rc), path);
            }
            boolean success = false;
                                                if (rc == Code.OK.intValue() || (local && rc != Code.OK.intValue())) {
                                if (watcher == null) {
                    Set<Watcher> pathWatchers = pathVsWatcher.remove(path);
                    if (pathWatchers != null) {
                                                removedWatchers.addAll(pathWatchers);
                        success = true;
                    }
                } else {
                    Set<Watcher> watchers = pathVsWatcher.get(path);
                    if (watchers != null) {
                        if (watchers.remove(watcher)) {
                                                        removedWatchers.add(watcher);
                                                        if (watchers.size() <= 0) {
                                pathVsWatcher.remove(path);
                            }
                            success = true;
                        }
                    }
                }
            }
            return success;
        }
        
        
        @Override
        public Set<Watcher> materialize(Watcher.Event.KeeperState state,
                                        Watcher.Event.EventType type,
                                        String clientPath)
        {
            Set<Watcher> result = new HashSet<Watcher>();

            switch (type) {
            case None:
                result.add(defaultWatcher);
                boolean clear = disableAutoWatchReset && state != Watcher.Event.KeeperState.SyncConnected;
                synchronized(dataWatches) {
                    for(Set<Watcher> ws: dataWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        dataWatches.clear();
                    }
                }

                synchronized(existWatches) {
                    for(Set<Watcher> ws: existWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        existWatches.clear();
                    }
                }

                synchronized(childWatches) {
                    for(Set<Watcher> ws: childWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        childWatches.clear();
                    }
                }

                return result;
            case NodeDataChanged:
            case NodeCreated:
                synchronized (dataWatches) {
                    addTo(dataWatches.remove(clientPath), result);
                }
                synchronized (existWatches) {
                    addTo(existWatches.remove(clientPath), result);
                }
                break;
            case NodeChildrenChanged:
                synchronized (childWatches) {
                    addTo(childWatches.remove(clientPath), result);
                }
                break;
            case NodeDeleted:
                synchronized (dataWatches) {
                    addTo(dataWatches.remove(clientPath), result);
                }
                                synchronized (existWatches) {
                    Set<Watcher> list = existWatches.remove(clientPath);
                    if (list != null) {
                        addTo(list, result);
                        LOG.warn("We are triggering an exists watch for delete! Shouldn't happen!");
                    }
                }
                synchronized (childWatches) {
                    addTo(childWatches.remove(clientPath), result);
                }
                break;
            default:
                String msg = "Unhandled watch event type " + type
                    + " with state " + state + " on path " + clientPath;
                LOG.error(msg);
                throw new RuntimeException(msg);
            }

            return result;
        }
    }

    
    public abstract class WatchRegistration {
        private Watcher watcher;
        private String clientPath;
        public WatchRegistration(Watcher watcher, String clientPath)
        {
            this.watcher = watcher;
            this.clientPath = clientPath;
        }

        abstract protected Map<String, Set<Watcher>> getWatches(int rc);

        
        public void register(int rc) {
            if (shouldAddWatch(rc)) {
                Map<String, Set<Watcher>> watches = getWatches(rc);
                synchronized(watches) {
                    Set<Watcher> watchers = watches.get(clientPath);
                    if (watchers == null) {
                        watchers = new HashSet<Watcher>();
                        watches.put(clientPath, watchers);
                    }
                    watchers.add(watcher);
                }
            }
        }
        
        protected boolean shouldAddWatch(int rc) {
            return rc == 0;
        }
    }

    
    class ExistsWatchRegistration extends WatchRegistration {
        public ExistsWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return rc == 0 ?  watchManager.dataWatches : watchManager.existWatches;
        }

        @Override
        protected boolean shouldAddWatch(int rc) {
            return rc == 0 || rc == KeeperException.Code.NONODE.intValue();
        }
    }

    class DataWatchRegistration extends WatchRegistration {
        public DataWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return watchManager.dataWatches;
        }
    }

    class ChildWatchRegistration extends WatchRegistration {
        public ChildWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            return watchManager.childWatches;
        }
    }

    @InterfaceAudience.Public
    public enum States {
        CONNECTING, ASSOCIATING, CONNECTED, CONNECTEDREADONLY,
        CLOSED, AUTH_FAILED, NOT_CONNECTED;

        public boolean isAlive() {
            return this != CLOSED && this != AUTH_FAILED;
        }

        
        public boolean isConnected() {
            return this == CONNECTED || this == CONNECTEDREADONLY;
        }
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException
    {
        this(connectString, sessionTimeout, watcher, false);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, false, conf);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, HostProvider aHostProvider)
            throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                aHostProvider, null);
    }


    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, HostProvider aHostProvider,
            ZKClientConfig clientConfig) throws IOException {
        LOG.info("Initiating client connection, connectString=" + connectString
                + " sessionTimeout=" + sessionTimeout + " watcher=" + watcher);

        if (clientConfig == null) {
            clientConfig = new ZKClientConfig();
        }
        this.clientConfig = clientConfig;
        watchManager = defaultWatchManager();
        watchManager.defaultWatcher = watcher;
        ConnectStringParser connectStringParser = new ConnectStringParser(
                connectString);
        hostProvider = aHostProvider;

        cnxn = createConnection(connectStringParser.getChrootPath(),
                hostProvider, sessionTimeout, this, watchManager,
                getClientCnxnSocket(), canBeReadOnly);
        cnxn.start();
    }

        protected ClientCnxn createConnection(String chrootPath,
            HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
            boolean canBeReadOnly) throws IOException {
        return new ClientCnxn(chrootPath, hostProvider, sessionTimeout, this,
                watchManager, clientCnxnSocket, canBeReadOnly);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                createDefaultHostProvider(connectString));
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                createDefaultHostProvider(connectString), conf);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd)
        throws IOException
    {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, false);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly,
            HostProvider aHostProvider) throws IOException {
    	this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd,
    			canBeReadOnly, aHostProvider, null);
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
    		long sessionId, byte[] sessionPasswd, boolean canBeReadOnly,
    		HostProvider aHostProvider, ZKClientConfig clientConfig) throws IOException {
    	LOG.info("Initiating client connection, connectString=" + connectString
    			+ " sessionTimeout=" + sessionTimeout
    			+ " watcher=" + watcher
    			+ " sessionId=" + Long.toHexString(sessionId)
    			+ " sessionPasswd="
    			+ (sessionPasswd == null ? "<null>" : "<hidden>"));

        if (clientConfig == null) {
            clientConfig = new ZKClientConfig();
        }
        this.clientConfig = clientConfig;
        watchManager = defaultWatchManager();
        watchManager.defaultWatcher = watcher;

        ConnectStringParser connectStringParser = new ConnectStringParser(
                connectString);
        hostProvider = aHostProvider;

        cnxn = new ClientCnxn(connectStringParser.getChrootPath(),
                hostProvider, sessionTimeout, this, watchManager,
                getClientCnxnSocket(), sessionId, sessionPasswd, canBeReadOnly);
        cnxn.seenRwServerBefore = true;         cnxn.start();
    }

    
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly)
            throws IOException {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd,
                canBeReadOnly, createDefaultHostProvider(connectString));
    }

        private static HostProvider createDefaultHostProvider(String connectString) {
        return new StaticHostProvider(
                new ConnectStringParser(connectString).getServerAddresses());
    }

        public Testable getTestable() {
        return new ZooKeeperTestable(this, cnxn);
    }

    
    protected ZKWatchManager defaultWatchManager() {
        return new ZKWatchManager(getClientConfig().getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET));
    }

    
    public long getSessionId() {
        return cnxn.getSessionId();
    }

    
    public byte[] getSessionPasswd() {
        return cnxn.getSessionPasswd();
    }

    
    public int getSessionTimeout() {
        return cnxn.getSessionTimeout();
    }

    
    public void addAuthInfo(String scheme, byte auth[]) {
        cnxn.addAuthInfo(scheme, auth);
    }

    
    public synchronized void register(Watcher watcher) {
        watchManager.defaultWatcher = watcher;
    }

    
    public synchronized void close() throws InterruptedException {
        if (!cnxn.getState().isAlive()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Close called on already closed client");
            }
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing session: 0x" + Long.toHexString(getSessionId()));
        }

        try {
            cnxn.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring unexpected exception during close", e);
            }
        }

        LOG.info("Session: 0x" + Long.toHexString(getSessionId()) + " closed");
    }

    
    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException {
        close();
        return testableWaitForShutdown(waitForShutdownTimeoutMs);
    }

    
    private String prependChroot(String clientPath) {
        if (cnxn.chrootPath != null) {
                        if (clientPath.length() == 1) {
                return cnxn.chrootPath;
            }
            return cnxn.chrootPath + clientPath;
        } else {
            return clientPath;
        }
    }

    
    public String create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, -1);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);
        CreateRequest request = new CreateRequest();
        CreateResponse response = new CreateResponse();
        request.setData(data);
        request.setFlags(createMode.toFlag());
        request.setPath(serverPath);
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
        request.setAcl(acl);
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (cnxn.chrootPath == null) {
            return response.getPath();
        } else {
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }

    
    public String create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Stat stat)
            throws KeeperException, InterruptedException {
        return create(path, data, acl, createMode, stat, -1);
    }

    
    public String create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Stat stat, long ttl)
            throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, ttl);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        Create2Response response = new Create2Response();
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException();
        }
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);
        ReplyHeader r = cnxn.submitRequest(h, record, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        if (cnxn.chrootPath == null) {
            return response.getPath();
        } else {
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }

    private void setCreateHeader(CreateMode createMode, RequestHeader h) {
        if (createMode.isTTL()) {
            h.setType(ZooDefs.OpCode.createTTL);
        } else {
            h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create2);
        }
    }

    private Record makeCreateRecord(CreateMode createMode, String serverPath, byte[] data, List<ACL> acl, long ttl) {
        Record record;
        if (createMode.isTTL()) {
            CreateTTLRequest request = new CreateTTLRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            request.setTtl(ttl);
            record = request;
        } else {
            CreateRequest request = new CreateRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            record = request;
        }
        return record;
    }

    
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, StringCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, -1);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);
        CreateRequest request = new CreateRequest();
        CreateResponse response = new CreateResponse();
        ReplyHeader r = new ReplyHeader();
        request.setData(data);
        request.setFlags(createMode.toFlag());
        request.setPath(serverPath);
        request.setAcl(acl);
        cnxn.queuePacket(h, r, request, response, cb, clientPath,
                serverPath, ctx, null);
    }

    
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Create2Callback cb, Object ctx)
    {
        create(path, data, acl, createMode, cb, ctx, -1);
    }

    
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Create2Callback cb, Object ctx, long ttl)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, ttl);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        ReplyHeader r = new ReplyHeader();
        Create2Response response = new Create2Response();
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);
        cnxn.queuePacket(h, r, record, response, cb, clientPath,
                serverPath, ctx, null);
    }

    
    public void delete(final String path, int version)
        throws InterruptedException, KeeperException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

                                if (clientPath.equals("/")) {
                                    serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.delete);
        DeleteRequest request = new DeleteRequest();
        request.setPath(serverPath);
        request.setVersion(version);
        ReplyHeader r = cnxn.submitRequest(h, request, null, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
    }

    
    public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
        for (Op op : ops) {
            op.validate();
        }
        return multiInternal(generateMultiTransaction(ops));
    }

    
    public void multi(Iterable<Op> ops, MultiCallback cb, Object ctx) {
        List<OpResult> results = validatePath(ops);
        if (results.size() > 0) {
            cb.processResult(KeeperException.Code.BADARGUMENTS.intValue(),
                    null, ctx, results);
            return;
        }
        multiInternal(generateMultiTransaction(ops), cb, ctx);
    }

    private List<OpResult> validatePath(Iterable<Op> ops) {
        List<OpResult> results = new ArrayList<OpResult>();
        boolean error = false;
        for (Op op : ops) {
            try {
                op.validate();
            } catch (IllegalArgumentException iae) {
                LOG.error("IllegalArgumentException: " + iae.getMessage());
                ErrorResult err = new ErrorResult(
                        KeeperException.Code.BADARGUMENTS.intValue());
                results.add(err);
                error = true;
                continue;
            } catch (KeeperException ke) {
                LOG.error("KeeperException: " + ke.getMessage());
                ErrorResult err = new ErrorResult(ke.code().intValue());
                results.add(err);
                error = true;
                continue;
            }
            ErrorResult err = new ErrorResult(
                    KeeperException.Code.RUNTIMEINCONSISTENCY.intValue());
            results.add(err);
        }
        if (false == error) {
            results.clear();
        }
        return results;
    }

    private MultiTransactionRecord generateMultiTransaction(Iterable<Op> ops) {
                List<Op> transaction = new ArrayList<Op>();
        for (Op op : ops) {
            transaction.add(withRootPrefix(op));
        }
        return new MultiTransactionRecord(transaction);
    }

    private Op withRootPrefix(Op op) {
        if (null != op.getPath()) {
            final String serverPath = prependChroot(op.getPath());
            if (!op.getPath().equals(serverPath)) {
                return op.withChroot(serverPath);
            }
        }
        return op;
    }

    protected void multiInternal(MultiTransactionRecord request, MultiCallback cb, Object ctx) {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.multi);
        MultiResponse response = new MultiResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, null, null, ctx, null);
    }

    protected List<OpResult> multiInternal(MultiTransactionRecord request)
        throws InterruptedException, KeeperException {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.multi);
        MultiResponse response = new MultiResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()));
        }

        List<OpResult> results = response.getResultList();
        
        ErrorResult fatalError = null;
        for (OpResult result : results) {
            if (result instanceof ErrorResult && ((ErrorResult)result).getErr() != KeeperException.Code.OK.intValue()) {
                fatalError = (ErrorResult) result;
                break;
            }
        }

        if (fatalError != null) {
            KeeperException ex = KeeperException.create(KeeperException.Code.get(fatalError.getErr()));
            ex.setMultiResults(results);
            throw ex;
        }

        return results;
    }

    
    public Transaction transaction() {
        return new Transaction(this);
    }

    
    public void delete(final String path, int version, VoidCallback cb,
            Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

                                if (clientPath.equals("/")) {
                                    serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.delete);
        DeleteRequest request = new DeleteRequest();
        request.setPath(serverPath);
        request.setVersion(version);
        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null);
    }

    
    public Stat exists(final String path, Watcher watcher)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.exists);
        ExistsRequest request = new ExistsRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        SetDataResponse response = new SetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            if (r.getErr() == KeeperException.Code.NONODE.intValue()) {
                return null;
            }
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }

        return response.getStat().getCzxid() == -1 ? null : response.getStat();
    }

    
    public Stat exists(String path, boolean watch) throws KeeperException,
        InterruptedException
    {
        return exists(path, watch ? watchManager.defaultWatcher : null);
    }

    
    public void exists(final String path, Watcher watcher,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.exists);
        ExistsRequest request = new ExistsRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        SetDataResponse response = new SetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    
    public byte[] getData(final String path, Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException
     {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }

    
    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getData(path, watch ? watchManager.defaultWatcher : null, stat);
    }

    
    public void getData(final String path, Watcher watcher,
            DataCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        getData(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    
    public byte[] getConfig(Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException
     {
        final String configZnode = ZooDefs.CONFIG_NODE;
 
                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                   configZnode);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }

    
    public void getConfig(Watcher watcher,
            DataCallback cb, Object ctx)
    {
        final String configZnode = ZooDefs.CONFIG_NODE;
        
                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
               configZnode, configZnode, ctx, wcb);
    }

    
    
    public byte[] getConfig(boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getConfig(watch ? watchManager.defaultWatcher : null, stat);
    }
 
    
    public void getConfig(boolean watch, DataCallback cb, Object ctx) {
        getConfig(watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    
    @Deprecated
    public byte[] reconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    
    @Deprecated
    public byte[] reconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    
    @Deprecated
    public void reconfig(String joiningServers, String leavingServers,
                         String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    
    @Deprecated
    public void reconfig(List<String> joiningServers,
                         List<String> leavingServers, List<String> newMembers, long fromConfig,
                         DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    
    public Stat setData(final String path, byte data[], int version)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getStat();
    }

    
    public void setData(final String path, byte data[], int version,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    
    public List<ACL> getACL(final String path, Stat stat)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getAcl();
    }

    
    public void getACL(final String path, Stat stat, ACLCallback cb,
            Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    
    public Stat setACL(final String path, List<ACL> acl, int aclVersion)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        if (acl != null && acl.size() == 0) {
            throw new KeeperException.InvalidACLException(clientPath);
        }
        request.setAcl(acl);
        request.setVersion(aclVersion);
        SetACLResponse response = new SetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getStat();
    }

    
    public void setACL(final String path, List<ACL> acl, int version,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        request.setAcl(acl);
        request.setVersion(version);
        SetACLResponse response = new SetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    
    public List<String> getChildren(final String path, Watcher watcher)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getChildren();
    }

    
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null);
    }

    
    public void getChildren(final String path, Watcher watcher,
            ChildrenCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    
    public void getChildren(String path, boolean watch, ChildrenCallback cb,
            Object ctx)
    {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    
    public List<String> getChildren(final String path, Watcher watcher,
            Stat stat)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getChildren();
    }

    
    public List<String> getChildren(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null,
                stat);
    }

    
    public void getChildren(final String path, Watcher watcher,
            Children2Callback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

                WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    
    public void getChildren(String path, boolean watch, Children2Callback cb,
            Object ctx)
    {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    
    public void sync(final String path, VoidCallback cb, Object ctx){
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.sync);
        SyncRequest request = new SyncRequest();
        SyncResponse response = new SyncResponse();
        request.setPath(serverPath);
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local)
            throws InterruptedException, KeeperException {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local);
    }

    
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local, cb, ctx);
    }

    
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local) throws InterruptedException, KeeperException {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null, watcherType,
                local);
    }

    
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local, VoidCallback cb, Object ctx) {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null,
                watcherType, local, cb, ctx);
    }

    private void validateWatcher(Watcher watcher) {
        if (watcher == null) {
            throw new IllegalArgumentException(
                    "Invalid Watcher, shouldn't be null!");
        }
    }

    private void removeWatches(int opCode, String path, Watcher watcher,
            WatcherType watcherType, boolean local)
            throws InterruptedException, KeeperException {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        ReplyHeader r = cnxn.submitRequest(h, request, null, null, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
    }

    private void removeWatches(int opCode, String path, Watcher watcher,
            WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null, wcb);
    }

    private Record getRemoveWatchesRequest(int opCode, WatcherType watcherType,
            final String serverPath) {
        Record request = null;
        switch (opCode) {
        case ZooDefs.OpCode.checkWatches:
            CheckWatchesRequest chkReq = new CheckWatchesRequest();
            chkReq.setPath(serverPath);
            chkReq.setType(watcherType.getIntValue());
            request = chkReq;
            break;
        case ZooDefs.OpCode.removeWatches:
            RemoveWatchesRequest rmReq = new RemoveWatchesRequest();
            rmReq.setPath(serverPath);
            rmReq.setType(watcherType.getIntValue());
            request = rmReq;
            break;
        default:
            LOG.warn("unknown type " + opCode);
            break;
        }
        return request;
    }

    public States getState() {
        return cnxn.getState();
    }

    
    @Override
    public String toString() {
        States state = getState();
        return ("State:" + state.toString()
                + (state.isConnected() ?
                        " Timeout:" + getSessionTimeout() + " " :
                        " ")
                + cnxn);
    }

    

    
    protected boolean testableWaitForShutdown(int wait)
        throws InterruptedException
    {
        cnxn.sendThread.join(wait);
        if (cnxn.sendThread.isAlive()) return false;
        cnxn.eventThread.join(wait);
        if (cnxn.eventThread.isAlive()) return false;
        return true;
    }

    
    protected SocketAddress testableRemoteSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getRemoteSocketAddress();
    }

    
    protected SocketAddress testableLocalSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getLocalSocketAddress();
    }

    private ClientCnxnSocket getClientCnxnSocket() throws IOException {
        String clientCnxnSocketName = getClientConfig().getProperty(
                ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        if (clientCnxnSocketName == null) {
            clientCnxnSocketName = ClientCnxnSocketNIO.class.getName();
        }
        try {
            Constructor<?> clientCxnConstructor = Class.forName(clientCnxnSocketName).getDeclaredConstructor(ZKClientConfig.class);
            ClientCnxnSocket clientCxnSocket = (ClientCnxnSocket) clientCxnConstructor.newInstance(getClientConfig());
            return clientCxnSocket;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate "
                    + clientCnxnSocketName);
            ioe.initCause(e);
            throw ioe;
        }
    }

    protected byte[] internalReconfig(String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.reconfig);
        ReconfigRequest request = new ReconfigRequest(joiningServers, leavingServers, newMembers, fromConfig);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()), "");
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }

    protected byte[] internalReconfig(List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(StringUtils.joinStrings(joiningServers, ","),
                StringUtils.joinStrings(leavingServers, ","),
                StringUtils.joinStrings(newMembers, ","),
                fromConfig, stat);
    }

    protected void internalReconfig(String joiningServers, String leavingServers,
                         String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.reconfig);
        ReconfigRequest request = new ReconfigRequest(joiningServers, leavingServers, newMembers, fromConfig);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                ZooDefs.CONFIG_NODE, ZooDefs.CONFIG_NODE, ctx, null);
    }

    protected void internalReconfig(List<String> joiningServers,
                         List<String> leavingServers, List<String> newMembers, long fromConfig,
                         DataCallback cb, Object ctx) {
        internalReconfig(StringUtils.joinStrings(joiningServers, ","),
                StringUtils.joinStrings(leavingServers, ","),
                StringUtils.joinStrings(newMembers, ","),
                fromConfig, cb, ctx);
    }
}
