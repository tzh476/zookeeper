package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
@InterfaceAudience.Public
public abstract class KeeperException extends Exception {
    
    private List<OpResult> results;

    
    public static KeeperException create(Code code, String path) {
        KeeperException r = create(code);
        r.path = path;
        return r;
    }

    
    @Deprecated
    public static KeeperException create(int code, String path) {
        KeeperException r = create(Code.get(code));
        r.path = path;
        return r;
    }

    
    @Deprecated
    public static KeeperException create(int code) {
        return create(Code.get(code));
    }

    
    public static KeeperException create(Code code) {
        switch (code) {
            case SYSTEMERROR:
                return new SystemErrorException();
            case RUNTIMEINCONSISTENCY:
                return new RuntimeInconsistencyException();
            case DATAINCONSISTENCY:
                return new DataInconsistencyException();
            case CONNECTIONLOSS:
                return new ConnectionLossException();
            case MARSHALLINGERROR:
                return new MarshallingErrorException();
            case UNIMPLEMENTED:
                return new UnimplementedException();
            case OPERATIONTIMEOUT:
                return new OperationTimeoutException();
            case NEWCONFIGNOQUORUM:
               return new NewConfigNoQuorum();
            case RECONFIGINPROGRESS:
               return new ReconfigInProgress();
            case BADARGUMENTS:
                return new BadArgumentsException();
            case APIERROR:
                return new APIErrorException();
            case NONODE:
                return new NoNodeException();
            case NOAUTH:
                return new NoAuthException();
            case BADVERSION:
                return new BadVersionException();
            case NOCHILDRENFOREPHEMERALS:
                return new NoChildrenForEphemeralsException();
            case NODEEXISTS:
                return new NodeExistsException();
            case INVALIDACL:
                return new InvalidACLException();
            case AUTHFAILED:
                return new AuthFailedException();
            case NOTEMPTY:
                return new NotEmptyException();
            case SESSIONEXPIRED:
                return new SessionExpiredException();
            case INVALIDCALLBACK:
                return new InvalidCallbackException();
            case SESSIONMOVED:
                return new SessionMovedException();
            case NOTREADONLY:
                return new NotReadOnlyException();
            case EPHEMERALONLOCALSESSION:
                return new EphemeralOnLocalSessionException();
            case NOWATCHER:
                return new NoWatcherException();
            case RECONFIGDISABLED:
                return new ReconfigDisabledException();
            case REQUESTTIMEOUT:
                return new RequestTimeoutException();
            case OK:
            default:
                throw new IllegalArgumentException("Invalid exception code");
        }
    }

    
    @Deprecated
    public void setCode(int code) {
        this.code = Code.get(code);
    }

    
    @Deprecated
    @InterfaceAudience.Public
    public interface CodeDeprecated {
        
        @Deprecated
        public static final int Ok = 0;

        
        @Deprecated
        public static final int SystemError = -1;
        
        @Deprecated
        public static final int RuntimeInconsistency = -2;
        
        @Deprecated
        public static final int DataInconsistency = -3;
        
        @Deprecated
        public static final int ConnectionLoss = -4;
        
        @Deprecated
        public static final int MarshallingError = -5;
        
        @Deprecated
        public static final int Unimplemented = -6;
        
        @Deprecated
        public static final int OperationTimeout = -7;
        
        @Deprecated
        public static final int BadArguments = -8;

        @Deprecated
        public static final int UnknownSession= -12;

        
        @Deprecated
        public static final int NewConfigNoQuorum = -13;

        
        @Deprecated
        public static final int ReconfigInProgress= -14;

        
        @Deprecated
        public static final int APIError = -100;

        
        @Deprecated
        public static final int NoNode = -101;
        
        @Deprecated
        public static final int NoAuth = -102;
        
        @Deprecated
        public static final int BadVersion = -103;
        
        @Deprecated
        public static final int NoChildrenForEphemerals = -108;
        
        @Deprecated
        public static final int NodeExists = -110;
        
        @Deprecated
        public static final int NotEmpty = -111;
        
        @Deprecated
        public static final int SessionExpired = -112;
        
        @Deprecated
        public static final int InvalidCallback = -113;
        
        @Deprecated
        public static final int InvalidACL = -114;
        
        @Deprecated
        public static final int AuthFailed = -115;
        
                        
        @Deprecated
        public static final int EphemeralOnLocalSession = -120;

    }

    
    @InterfaceAudience.Public
    public static enum Code implements CodeDeprecated {
        
        OK (Ok),

        
        SYSTEMERROR (SystemError),

        
        RUNTIMEINCONSISTENCY (RuntimeInconsistency),
        
        DATAINCONSISTENCY (DataInconsistency),
        
        CONNECTIONLOSS (ConnectionLoss),
        
        MARSHALLINGERROR (MarshallingError),
        
        UNIMPLEMENTED (Unimplemented),
        
        OPERATIONTIMEOUT (OperationTimeout),
        
        BADARGUMENTS (BadArguments),
        
        NEWCONFIGNOQUORUM (NewConfigNoQuorum),
        
        RECONFIGINPROGRESS (ReconfigInProgress),
        
        UNKNOWNSESSION (UnknownSession),
        
        
        APIERROR (APIError),

        
        NONODE (NoNode),
        
        NOAUTH (NoAuth),
        
        BADVERSION (BadVersion),
        
        NOCHILDRENFOREPHEMERALS (NoChildrenForEphemerals),
        
        NODEEXISTS (NodeExists),
        
        NOTEMPTY (NotEmpty),
        
        SESSIONEXPIRED (SessionExpired),
        
        INVALIDCALLBACK (InvalidCallback),
        
        INVALIDACL (InvalidACL),
        
        AUTHFAILED (AuthFailed),
        
        SESSIONMOVED (-118),
        
        NOTREADONLY (-119),
        
        EPHEMERALONLOCALSESSION (EphemeralOnLocalSession),
        
        NOWATCHER (-121),
        
        REQUESTTIMEOUT (-122),
        
        RECONFIGDISABLED(-123);

        private static final Map<Integer,Code> lookup
            = new HashMap<Integer,Code>();

        static {
            for(Code c : EnumSet.allOf(Code.class))
                lookup.put(c.code, c);
        }

        private final int code;
        Code(int code) {
            this.code = code;
        }

        
        public int intValue() { return code; }

        
        public static Code get(int code) {
            return lookup.get(code);
        }
    }

    static String getCodeMessage(Code code) {
        switch (code) {
            case OK:
                return "ok";
            case SYSTEMERROR:
                return "SystemError";
            case RUNTIMEINCONSISTENCY:
                return "RuntimeInconsistency";
            case DATAINCONSISTENCY:
                return "DataInconsistency";
            case CONNECTIONLOSS:
                return "ConnectionLoss";
            case MARSHALLINGERROR:
                return "MarshallingError";
            case NEWCONFIGNOQUORUM:
               return "NewConfigNoQuorum";
            case RECONFIGINPROGRESS:
               return "ReconfigInProgress";
            case UNIMPLEMENTED:
                return "Unimplemented";
            case OPERATIONTIMEOUT:
                return "OperationTimeout";
            case BADARGUMENTS:
                return "BadArguments";
            case APIERROR:
                return "APIError";
            case NONODE:
                return "NoNode";
            case NOAUTH:
                return "NoAuth";
            case BADVERSION:
                return "BadVersion";
            case NOCHILDRENFOREPHEMERALS:
                return "NoChildrenForEphemerals";
            case NODEEXISTS:
                return "NodeExists";
            case INVALIDACL:
                return "InvalidACL";
            case AUTHFAILED:
                return "AuthFailed";
            case NOTEMPTY:
                return "Directory not empty";
            case SESSIONEXPIRED:
                return "Session expired";
            case INVALIDCALLBACK:
                return "Invalid callback";
            case SESSIONMOVED:
                return "Session moved";
            case NOTREADONLY:
                return "Not a read-only call";
            case EPHEMERALONLOCALSESSION:
                return "Ephemeral node on local session";
            case NOWATCHER:
                return "No such watcher";
            case RECONFIGDISABLED:
                return "Reconfig is disabled";
            default:
                return "Unknown error " + code;
        }
    }

    private Code code;

    private String path;

    public KeeperException(Code code) {
        this.code = code;
    }

    KeeperException(Code code, String path) {
        this.code = code;
        this.path = path;
    }

    
    @Deprecated
    public int getCode() {
        return code.code;
    }

    
    public Code code() {
        return code;
    }

    
    public String getPath() {
        return path;
    }

    @Override
    public String getMessage() {
        if (path == null || path.isEmpty()) {
            return "KeeperErrorCode = " + getCodeMessage(code);
        }
        return "KeeperErrorCode = " + getCodeMessage(code) + " for " + path;
    }

    void setMultiResults(List<OpResult> results) {
        this.results = results;
    }

    
    public List<OpResult> getResults() {
        return results != null ? new ArrayList<OpResult>(results) : null;
    }

    
    @InterfaceAudience.Public
    public static class APIErrorException extends KeeperException {
        public APIErrorException() {
            super(Code.APIERROR);
        }
    }

    
    @InterfaceAudience.Public
    public static class AuthFailedException extends KeeperException {
        public AuthFailedException() {
            super(Code.AUTHFAILED);
        }
    }

    
    @InterfaceAudience.Public
    public static class BadArgumentsException extends KeeperException {
        public BadArgumentsException() {
            super(Code.BADARGUMENTS);
        }
        public BadArgumentsException(String path) {
            super(Code.BADARGUMENTS, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class BadVersionException extends KeeperException {
        public BadVersionException() {
            super(Code.BADVERSION);
        }
        public BadVersionException(String path) {
            super(Code.BADVERSION, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class ConnectionLossException extends KeeperException {
        public ConnectionLossException() {
            super(Code.CONNECTIONLOSS);
        }
    }

    
    @InterfaceAudience.Public
    public static class DataInconsistencyException extends KeeperException {
        public DataInconsistencyException() {
            super(Code.DATAINCONSISTENCY);
        }
    }

    
    @InterfaceAudience.Public
    public static class InvalidACLException extends KeeperException {
        public InvalidACLException() {
            super(Code.INVALIDACL);
        }
        public InvalidACLException(String path) {
            super(Code.INVALIDACL, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class InvalidCallbackException extends KeeperException {
        public InvalidCallbackException() {
            super(Code.INVALIDCALLBACK);
        }
    }

    
    @InterfaceAudience.Public
    public static class MarshallingErrorException extends KeeperException {
        public MarshallingErrorException() {
            super(Code.MARSHALLINGERROR);
        }
    }

    
    @InterfaceAudience.Public
    public static class NoAuthException extends KeeperException {
        public NoAuthException() {
            super(Code.NOAUTH);
        }
    }

    
    @InterfaceAudience.Public
    public static class NewConfigNoQuorum extends KeeperException {
        public NewConfigNoQuorum() {
            super(Code.NEWCONFIGNOQUORUM);
        }
    }
    
    
    @InterfaceAudience.Public
    public static class ReconfigInProgress extends KeeperException {
        public ReconfigInProgress() {
            super(Code.RECONFIGINPROGRESS);
        }
    }
    
    
    @InterfaceAudience.Public
    public static class NoChildrenForEphemeralsException extends KeeperException {
        public NoChildrenForEphemeralsException() {
            super(Code.NOCHILDRENFOREPHEMERALS);
        }
        public NoChildrenForEphemeralsException(String path) {
            super(Code.NOCHILDRENFOREPHEMERALS, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class NodeExistsException extends KeeperException {
        public NodeExistsException() {
            super(Code.NODEEXISTS);
        }
        public NodeExistsException(String path) {
            super(Code.NODEEXISTS, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class NoNodeException extends KeeperException {
        public NoNodeException() {
            super(Code.NONODE);
        }
        public NoNodeException(String path) {
            super(Code.NONODE, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class NotEmptyException extends KeeperException {
        public NotEmptyException() {
            super(Code.NOTEMPTY);
        }
        public NotEmptyException(String path) {
            super(Code.NOTEMPTY, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class OperationTimeoutException extends KeeperException {
        public OperationTimeoutException() {
            super(Code.OPERATIONTIMEOUT);
        }
    }

    
    @InterfaceAudience.Public
    public static class RuntimeInconsistencyException extends KeeperException {
        public RuntimeInconsistencyException() {
            super(Code.RUNTIMEINCONSISTENCY);
        }
    }

    
    @InterfaceAudience.Public
    public static class SessionExpiredException extends KeeperException {
        public SessionExpiredException() {
            super(Code.SESSIONEXPIRED);
        }
    }

    
    @InterfaceAudience.Public
    public static class UnknownSessionException extends KeeperException {
        public UnknownSessionException() {
            super(Code.UNKNOWNSESSION);
        }
    }

    
    @InterfaceAudience.Public
    public static class SessionMovedException extends KeeperException {
        public SessionMovedException() {
            super(Code.SESSIONMOVED);
        }
    }

    
    @InterfaceAudience.Public
    public static class NotReadOnlyException extends KeeperException {
        public NotReadOnlyException() {
            super(Code.NOTREADONLY);
        }
    }

    
    @InterfaceAudience.Public
    public static class EphemeralOnLocalSessionException extends KeeperException {
        public EphemeralOnLocalSessionException() {
            super(Code.EPHEMERALONLOCALSESSION);
        }
    }

    
    @InterfaceAudience.Public
    public static class SystemErrorException extends KeeperException {
        public SystemErrorException() {
            super(Code.SYSTEMERROR);
        }
    }

    
    @InterfaceAudience.Public
    public static class UnimplementedException extends KeeperException {
        public UnimplementedException() {
            super(Code.UNIMPLEMENTED);
        }
    }

    
    @InterfaceAudience.Public
    public static class NoWatcherException extends KeeperException {
        public NoWatcherException() {
            super(Code.NOWATCHER);
        }

        public NoWatcherException(String path) {
            super(Code.NOWATCHER, path);
        }
    }

    
    @InterfaceAudience.Public
    public static class ReconfigDisabledException extends KeeperException {
        public ReconfigDisabledException() { super(Code.RECONFIGDISABLED); }
        public ReconfigDisabledException(String path) {
            super(Code.RECONFIGDISABLED, path);
        }
    }

    
    public static class RequestTimeoutException extends KeeperException {
        public RequestTimeoutException() {
            super(Code.REQUESTTIMEOUT);
        }
    }
}
