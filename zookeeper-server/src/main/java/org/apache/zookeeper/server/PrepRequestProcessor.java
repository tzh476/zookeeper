package org.apache.zookeeper.server;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ReconfigRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class PrepRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    private static  boolean failCreate = false;

    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();

    private final RequestProcessor nextProcessor;

    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("ProcessThread(sid:" + zks.getServerId() + " cport:"
                + zks.getClientPort() + "):", zks.getZooKeeperServerListener());
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }
    @Override
    public void run() {
        try {
            while (true) {
                Request request = submittedRequests.take();
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                if (Request.requestOfDeath == request) {
                    break;
                }
                pRequest(request);
            }
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    private ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path);
            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Set<String> children;
                    synchronized(n) {
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat, children.size(),
                            zks.getZKDatabase().aclForNode(n));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    private ChangeRecord getOutstandingChange(String path) {
        synchronized (zks.outstandingChanges) {
            return zks.outstandingChangesForPath.get(path);
        }
    }

    private void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    
    private Map<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();

        for (Op op : multiRequest) {
            String path = op.getPath();
            ChangeRecord cr = getOutstandingChange(path);
                        if (cr != null) {
                pendingChangeRecords.put(path, cr);
            }

            
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1 || path.indexOf('\0') != -1) {
                continue;
            }
            String parentPath = path.substring(0, lastSlash);
            ChangeRecord parentCr = getOutstandingChange(parentPath);
            if (parentCr != null) {
                pendingChangeRecords.put(parentPath, parentCr);
            }
        }

        return pendingChangeRecords;
    }

    
    void rollbackPendingChanges(long zxid, Map<String, ChangeRecord>pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
                        Iterator<ChangeRecord> iter = zks.outstandingChanges.descendingIterator();
            while (iter.hasNext()) {
                ChangeRecord c = iter.next();
                if (c.zxid == zxid) {
                    iter.remove();
                                                            zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

                        if (zks.outstandingChanges.isEmpty()) {
                return;
            }

            long firstZxid = zks.outstandingChanges.peek().zxid;

            for (ChangeRecord c : pendingChangeRecords.values()) {
                                                                if (c.zxid < firstZxid) {
                    continue;
                }

                                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    
    static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm,
            List<Id> ids) throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Permission requested: {} ", perm);
            LOG.debug("ACLs for node: {}", acl);
            LOG.debug("Client credentials: {}", ids);
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap != null) {
                    for (Id authId : ids) {
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    
    private String validatePathForCreate(String path, long sessionId)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
            LOG.info("Invalid path %s with session 0x%s",
                    path, Long.toHexString(sessionId));
            throw new KeeperException.BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    
    protected void pRequest2Txn(int type, long zxid, Request request,
                                Record record, boolean deserialize)
        throws KeeperException, IOException, RequestProcessorException
    {
        request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                Time.currentWallTime(), type));

        switch (type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer: {
                pRequest2TxnCreate(type, request, record, deserialize);
                break;
            }
            case OpCode.deleteContainer: {
                String path = new String(request.request.array());
                String parentPath = getParentPathAndValidate(path);
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                if (EphemeralType.get(nodeRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL) {
                    throw new KeeperException.BadVersionException(path);
                }
                request.setTxn(new DeleteTxn(path));
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            }
            case OpCode.delete:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                DeleteRequest deleteRequest = (DeleteRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                String path = deleteRequest.getPath();
                String parentPath = getParentPathAndValidate(path);
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE, request.authInfo);
                checkAndIncVersion(nodeRecord.stat.getVersion(), deleteRequest.getVersion(), path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                request.setTxn(new DeleteTxn(path));
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            case OpCode.setData:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetDataRequest setDataRequest = (SetDataRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                path = setDataRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);
                int newVersion = checkAndIncVersion(nodeRecord.stat.getVersion(), setDataRequest.getVersion(), path);
                request.setTxn(new SetDataTxn(path, setDataRequest.getData(), newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.reconfig:
                if (!QuorumPeerConfig.isReconfigEnabled()) {
                    LOG.error("Reconfig operation requested but reconfig feature is disabled.");
                    throw new KeeperException.ReconfigDisabledException();
                }

                if (skipACL) {
                    LOG.warn("skipACL is set, reconfig operation will skip ACL checks!");
                }

                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                ReconfigRequest reconfigRequest = (ReconfigRequest)record; 
                LeaderZooKeeperServer lzks;
                try {
                    lzks = (LeaderZooKeeperServer)zks;
                } catch (ClassCastException e) {
                                        throw new KeeperException.UnimplementedException();
                }
                QuorumVerifier lastSeenQV = lzks.self.getLastSeenQuorumVerifier();                                                                                 
                                if (lastSeenQV.getVersion()!=lzks.self.getQuorumVerifier().getVersion()) {
                       throw new KeeperException.ReconfigInProgress(); 
                }
                long configId = reconfigRequest.getCurConfigId();
  
                if (configId != -1 && configId!=lzks.self.getLastSeenQuorumVerifier().getVersion()){
                   String msg = "Reconfiguration from version " + configId + " failed -- last seen version is " +
                           lzks.self.getLastSeenQuorumVerifier().getVersion();
                   throw new KeeperException.BadVersionException(msg);
                }

                String newMembers = reconfigRequest.getNewMembers();
                
                if (newMembers != null) {                    LOG.info("Non-incremental reconfig");
                
                                      newMembers = newMembers.replaceAll(",", "\n");
                   
                   try{
                       Properties props = new Properties();                        
                       props.load(new StringReader(newMembers));
                       request.qv = QuorumPeerConfig.parseDynamicConfig(props, lzks.self.getElectionType(), true, false);
                       request.qv.setVersion(request.getHdr().getZxid());
                   } catch (IOException | ConfigException e) {
                       throw new KeeperException.BadArgumentsException(e.getMessage());
                   }
                } else {                    LOG.info("Incremental reconfig");
                   
                   List<String> joiningServers = null; 
                   String joiningServersString = reconfigRequest.getJoiningServers();
                   if (joiningServersString != null)
                   {
                       joiningServers = StringUtils.split(joiningServersString,",");
                   }
                   
                   List<String> leavingServers = null;
                   String leavingServersString = reconfigRequest.getLeavingServers();
                   if (leavingServersString != null)
                   {
                       leavingServers = StringUtils.split(leavingServersString, ",");
                   }
                   
                   if (!(lastSeenQV instanceof QuorumMaj)) {
                           String msg = "Incremental reconfiguration requested but last configuration seen has a non-majority quorum system";
                           LOG.warn(msg);
                           throw new KeeperException.BadArgumentsException(msg);               
                   }
                   Map<Long, QuorumServer> nextServers = new HashMap<Long, QuorumServer>(lastSeenQV.getAllMembers());
                   try {                           
                       if (leavingServers != null) {
                           for (String leaving: leavingServers){
                               long sid = Long.parseLong(leaving);
                               nextServers.remove(sid);
                           } 
                       }
                       if (joiningServers != null) {
                           for (String joiner: joiningServers){
                        	                           	   String[] parts = StringUtils.split(joiner, "=").toArray(new String[0]);
                               if (parts.length != 2) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string");
                               }
                                                              Long sid = Long.parseLong(parts[0].substring(parts[0].lastIndexOf('.') + 1));
                               QuorumServer qs = new QuorumServer(sid, parts[1]);
                               if (qs.clientAddr == null || qs.electionAddr == null || qs.addr == null) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string - each server should have 3 ports specified"); 	   
                               }

                                                              for (QuorumServer nqs: nextServers.values()) {
                                   if (qs.id == nqs.id) {
                                       continue;
                                   }
                                   qs.checkAddressDuplicate(nqs);
                               }

                               nextServers.remove(qs.id);
                               nextServers.put(qs.id, qs);
                           }  
                       }
                   } catch (ConfigException e){
                       throw new KeeperException.BadArgumentsException("Reconfiguration failed");
                   }
                   request.qv = new QuorumMaj(nextServers);
                   request.qv.setVersion(request.getHdr().getZxid());
                }
                if (QuorumPeerConfig.isStandaloneEnabled() && request.qv.getVotingMembers().size() < 2) {
                   String msg = "Reconfig failed - new configuration must include at least 2 followers";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                } else if (request.qv.getVotingMembers().size() < 1) {
                   String msg = "Reconfig failed - new configuration must include at least 1 follower";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                }                           
                   
                if (!lzks.getLeader().isQuorumSynced(request.qv)) {
                   String msg2 = "Reconfig failed - there must be a connected and synced quorum in new configuration";
                   LOG.warn(msg2);             
                   throw new KeeperException.NewConfigNoQuorum();
                }
                
                nodeRecord = getRecordForPath(ZooDefs.CONFIG_NODE);               
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);                  
                request.setTxn(new SetDataTxn(ZooDefs.CONFIG_NODE, request.qv.toString().getBytes(), -1));    
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(-1);                
                addChangeRecord(nodeRecord);
                break;                         
            case OpCode.setACL:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                path = setAclRequest.getPath();
                validatePath(path, request.sessionId);
                List<ACL> listACL = fixupACL(path, request.authInfo, setAclRequest.getAcl());
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN, request.authInfo);
                newVersion = checkAndIncVersion(nodeRecord.stat.getAversion(), setAclRequest.getVersion(), path);
                request.setTxn(new SetACLTxn(path, listACL, newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setAversion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:
                request.request.rewind();
                int to = request.request.getInt();
                request.setTxn(new CreateSessionTxn(to));
                request.request.rewind();
                if (request.isLocalSession()) {
                                        zks.sessionTracker.addSession(request.sessionId, to);
                } else {
                                        zks.sessionTracker.addGlobalSession(request.sessionId, to);
                }
                zks.setOwner(request.sessionId, request.getOwner());
                break;
            case OpCode.closeSession:
                                                                                Set<String> es = zks.getZKDatabase()
                        .getEphemerals(request.sessionId);
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                                                        es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path2Delete, null, 0, null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }
                break;
            case OpCode.check:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                path = checkVersionRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ, request.authInfo);
                request.setTxn(new CheckVersionTxn(path, checkAndIncVersion(nodeRecord.stat.getVersion(),
                        checkVersionRequest.getVersion(), path)));
                break;
            default:
                LOG.warn("unknown type " + type);
                break;
        }
    }

    private void pRequest2TxnCreate(int type, Request request, Record record, boolean deserialize) throws IOException, KeeperException {
        if (deserialize) {
            ByteBufferInputStream.byteBuffer2Record(request.request, record);
        }

        int flags;
        String path;
        List<ACL> acl;
        byte[] data;
        long ttl;
        if (type == OpCode.createTTL) {
            CreateTTLRequest createTtlRequest = (CreateTTLRequest)record;
            flags = createTtlRequest.getFlags();
            path = createTtlRequest.getPath();
            acl = createTtlRequest.getAcl();
            data = createTtlRequest.getData();
            ttl = createTtlRequest.getTtl();
        } else {
            CreateRequest createRequest = (CreateRequest)record;
            flags = createRequest.getFlags();
            path = createRequest.getPath();
            acl = createRequest.getAcl();
            data = createRequest.getData();
            ttl = -1;
        }
        CreateMode createMode = CreateMode.fromFlag(flags);
        validateCreateRequest(path, createMode, request, ttl);
        String parentPath = validatePathForCreate(path, request.sessionId);

        List<ACL> listACL = fixupACL(path, request.authInfo, acl);
        ChangeRecord parentRecord = getRecordForPath(parentPath);

        checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE, request.authInfo);
        int parentCVersion = parentRecord.stat.getCversion();
        if (createMode.isSequential()) {
            path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
        }
        validatePath(path, request.sessionId);
        try {
            if (getRecordForPath(path) != null) {
                throw new KeeperException.NodeExistsException(path);
            }
        } catch (KeeperException.NoNodeException e) {
                    }
        boolean ephemeralParent = EphemeralType.get(parentRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL;
        if (ephemeralParent) {
            throw new KeeperException.NoChildrenForEphemeralsException(path);
        }
        int newCversion = parentRecord.stat.getCversion()+1;
        if (type == OpCode.createContainer) {
            request.setTxn(new CreateContainerTxn(path, data, listACL, newCversion));
        } else if (type == OpCode.createTTL) {
            request.setTxn(new CreateTTLTxn(path, data, listACL, newCversion, ttl));
        } else {
            request.setTxn(new CreateTxn(path, data, listACL, createMode.isEphemeral(),
                    newCversion));
        }
        StatPersisted s = new StatPersisted();
        if (createMode.isEphemeral()) {
            s.setEphemeralOwner(request.sessionId);
        }
        parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
        parentRecord.childCount++;
        parentRecord.stat.setCversion(newCversion);
        addChangeRecord(parentRecord);
        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, s, 0, listACL));
    }

    private void validatePath(String path, long sessionId) throws BadArgumentsException {
        try {
            PathUtils.validatePath(path);
        } catch(IllegalArgumentException ie) {
            LOG.info("Invalid path {} with session 0x{}, reason: {}",
                    path, Long.toHexString(sessionId), ie.getMessage());
            throw new BadArgumentsException(path);
        }
    }

    private String getParentPathAndValidate(String path)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1
                || zks.getZKDatabase().isSpecialPath(path)) {
            throw new BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    private static int checkAndIncVersion(int currentVersion, int expectedVersion, String path)
            throws KeeperException.BadVersionException {
        if (expectedVersion != -1 && expectedVersion != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }
        return currentVersion + 1;
    }

    
    protected void pRequest(Request request) throws RequestProcessorException {
                        request.setHdr(null);
        request.setTxn(null);

        try {
            switch (request.type) {
            case OpCode.createContainer:
            case OpCode.create:
            case OpCode.create2:
                CreateRequest create2Request = new CreateRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, create2Request, true);
                break;
            case OpCode.createTTL:
                CreateTTLRequest createTtlRequest = new CreateTTLRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, createTtlRequest, true);
                break;
            case OpCode.deleteContainer:
            case OpCode.delete:
                DeleteRequest deleteRequest = new DeleteRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                break;
            case OpCode.setData:
                SetDataRequest setDataRequest = new SetDataRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                break;
            case OpCode.reconfig:
                ReconfigRequest reconfigRequest = new ReconfigRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, reconfigRequest);
                pRequest2Txn(request.type, zks.getNextZxid(), request, reconfigRequest, true);
                break;
            case OpCode.setACL:
                SetACLRequest setAclRequest = new SetACLRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                break;
            case OpCode.check:
                CheckVersionRequest checkRequest = new CheckVersionRequest();              
                pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                break;
            case OpCode.multi:
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                try {
                    ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                } catch(IOException e) {
                    request.setHdr(new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                            Time.currentWallTime(), OpCode.multi));
                    throw e;
                }
                List<Txn> txns = new ArrayList<Txn>();
                                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                                Map<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                for(Op op: multiRequest) {
                    Record subrequest = op.toRequestRecord();
                    int type;
                    Record txn;

                    
                    if (ke != null) {
                        type = OpCode.error;
                        txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    }

                    
                    else {
                        try {
                            pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                            type = request.getHdr().getType();
                            txn = request.getTxn();
                        } catch (KeeperException e) {
                            ke = e;
                            type = OpCode.error;
                            txn = new ErrorTxn(e.code().intValue());

                            if (e.code().intValue() > Code.APIERROR.intValue()) {
                                LOG.info("Got user-level KeeperException when processing {} aborting" +
                                        " remaining multi ops. Error Path:{} Error:{}",
                                        request.toString(), e.getPath(), e.getMessage());
                            }

                            request.setException(e);

                            
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                                                                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                    txn.serialize(boa, "request") ;
                    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                    txns.add(new Txn(type, bb.array()));
                }

                request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                        Time.currentWallTime(), request.type));
                request.setTxn(new MultiTxn(txns));

                break;

                        case OpCode.createSession:
            case OpCode.closeSession:
                if (!request.isLocalSession()) {
                    pRequest2Txn(request.type, zks.getNextZxid(), request,
                                 null, true);
                }
                break;

                        case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
            case OpCode.checkWatches:
            case OpCode.removeWatches:
                zks.sessionTracker.checkSession(request.sessionId,
                        request.getOwner());
                break;
            default:
                LOG.warn("unknown type " + request.type);
                break;
            }
        } catch (KeeperException e) {
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(e.code().intValue()));
            }

            if (e.code().intValue() > Code.APIERROR.intValue()) {
                LOG.info("Got user-level KeeperException when processing {} Error Path:{} Error:{}",
                        request.toString(), e.getPath(), e.getMessage());
            }
            request.setException(e);
        } catch (Exception e) {
                                    LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(Code.MARSHALLINGERROR.intValue()));
            }
        }
        request.zxid = zks.getZxid();
        nextProcessor.processRequest(request);
    }

    private List<ACL> removeDuplicates(List<ACL> acl) {

        LinkedList<ACL> retval = new LinkedList<ACL>();
        for (ACL a : acl) {
            if (!retval.contains(a)) {
                retval.add(a);
            }
        }
        return retval;
    }

    private void validateCreateRequest(String path, CreateMode createMode, Request request, long ttl)
            throws KeeperException {
        if (createMode.isTTL() && !EphemeralType.extendedEphemeralTypesEnabled()) {
            throw new KeeperException.UnimplementedException();
        }
        try {
            EphemeralType.validateTTL(createMode, ttl);
        } catch (IllegalArgumentException e) {
            throw new BadArgumentsException(path);
        }
        if (createMode.isEphemeral()) {
                                    if (request.getException() != null) {
                throw request.getException();
            }
            zks.sessionTracker.checkGlobalSession(request.sessionId,
                    request.getOwner());
        } else {
            zks.sessionTracker.checkSession(request.sessionId,
                    request.getOwner());
        }
    }

    
    private List<ACL> fixupACL(String path, List<Id> authInfo, List<ACL> acls)
        throws KeeperException.InvalidACLException {
                        List<ACL> uniqacls = removeDuplicates(acls);
        LinkedList<ACL> rv = new LinkedList<ACL>();
        if (uniqacls == null || uniqacls.size() == 0) {
            throw new KeeperException.InvalidACLException(path);
        }
        for (ACL a: uniqacls) {
            LOG.debug("Processing ACL: {}", a);
            if (a == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            Id id = a.getId();
            if (id == null || id.getScheme() == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                rv.add(a);
            } else if (id.getScheme().equals("auth")) {
                                                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    AuthenticationProvider ap =
                        ProviderRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                            + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        rv.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    throw new KeeperException.InvalidACLException(path);
                }
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id.getScheme());
                if (ap == null || !ap.isValid(id.getId())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                rv.add(a);
            }
        }
        return rv;
    }

    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
