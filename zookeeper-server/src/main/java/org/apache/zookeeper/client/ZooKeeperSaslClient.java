package org.apache.zookeeper.client;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Login;
import org.apache.zookeeper.SaslClientCallbackHandler;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.SetSASLResponse;

import org.apache.zookeeper.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZooKeeperSaslClient {
    
    @Deprecated
    public static final String LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";
    
    @Deprecated
    public static final String ENABLE_CLIENT_SASL_KEY = "zookeeper.sasl.client";
    
    @Deprecated
    public static final String ENABLE_CLIENT_SASL_DEFAULT = "true";
    private volatile boolean initializedLogin = false; 

    
    @Deprecated
    public static boolean isEnabled() {
        return Boolean.valueOf(System.getProperty(
                ZKClientConfig.ENABLE_CLIENT_SASL_KEY,
                ZKClientConfig.ENABLE_CLIENT_SASL_DEFAULT));
    }

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSaslClient.class);
    private Login login = null;
    private SaslClient saslClient;
    private boolean isSASLConfigured = true;
    private final ZKClientConfig clientConfig;

    private byte[] saslToken = new byte[0];

    public enum SaslState {
        INITIAL,INTERMEDIATE,COMPLETE,FAILED
    }

    private SaslState saslState = SaslState.INITIAL;

    private boolean gotLastPacket = false;
    
    private final String configStatus;

    public SaslState getSaslState() {
        return saslState;
    }

    public String getLoginContext() {
        if (login != null)
            return login.getLoginContextName();
        return null;
    }

    public ZooKeeperSaslClient(final String serverPrincipal, ZKClientConfig clientConfig) throws LoginException {
        
        String clientSection = clientConfig.getProperty(
                ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT);
        this.clientConfig = clientConfig;
                AppConfigurationEntry entries[] = null;
        RuntimeException runtimeException = null;
        try {
            entries = Configuration.getConfiguration()
                    .getAppConfigurationEntry(clientSection);
        } catch (SecurityException e) {
                        runtimeException = e;
        } catch (IllegalArgumentException e) {
                                                runtimeException = e;
        }
        if (entries != null) {
            this.configStatus = "Will attempt to SASL-authenticate using Login Context section '" + clientSection + "'";
            this.saslClient = createSaslClient(serverPrincipal, clientSection);
        } else {
                                    saslState = SaslState.FAILED;
            String explicitClientSection = clientConfig
                    .getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY);
            if (explicitClientSection != null) {
                                                if (runtimeException != null) {
                    throw new LoginException(
                            "Zookeeper client cannot authenticate using the "
                                    + explicitClientSection
                                    + " section of the supplied JAAS configuration: '"
                                    + clientConfig.getJaasConfKey() + "' because of a "
                                    + "RuntimeException: " + runtimeException);
                } else {
                    throw new LoginException("Client cannot SASL-authenticate because the specified JAAS configuration " +
                            "section '" + explicitClientSection + "' could not be found.");
                }
            } else {
                                                String msg = "Will not attempt to authenticate using SASL ";
                if (runtimeException != null) {
                    msg += "(" + runtimeException + ")";
                } else {
                    msg += "(unknown error)";
                }
                this.configStatus = msg;
                this.isSASLConfigured = false;
            }
            if (clientConfig.getJaasConfKey() != null) {
                                                if (runtimeException != null) {
                    throw new LoginException(
                            "Zookeeper client cannot authenticate using the '"
                                    + clientConfig.getProperty(
                                            ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                                            ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT)
                                    + "' section of the supplied JAAS configuration: '"
                                    + clientConfig.getJaasConfKey() + "' because of a "
                                    + "RuntimeException: " + runtimeException);
                } else {
                    throw new LoginException(
                            "No JAAS configuration section named '"
                                    + clientConfig.getProperty(
                                            ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                                            ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT)
                                    + "' was found in specified JAAS configuration file: '"
                                    + clientConfig.getJaasConfKey() + "'.");
                }
            }
        }
    }
    
    
    public String getConfigStatus() {
        return configStatus;
    }

    public boolean isComplete() {
        return (saslState == SaslState.COMPLETE);
    }

    public boolean isFailed() {
        return (saslState == SaslState.FAILED);
    }

    public static class ServerSaslResponseCallback implements AsyncCallback.DataCallback {
        public void processResult(int rc, String path, Object ctx, byte data[], Stat stat) {
                                                            ZooKeeperSaslClient client = ((ClientCnxn)ctx).zooKeeperSaslClient;
            if (client == null) {
                LOG.warn("sasl client was unexpectedly null: cannot respond to Zookeeper server.");
                return;
            }
            byte[] usedata = data;
            if (data != null) {
                LOG.debug("ServerSaslResponseCallback(): saslToken server response: (length="+usedata.length+")");
            }
            else {
                usedata = new byte[0];
                LOG.debug("ServerSaslResponseCallback(): using empty data[] as server response (length="+usedata.length+")");
            }
            client.respondToServer(usedata, (ClientCnxn)ctx);
        }
    }

    private SaslClient createSaslClient(final String servicePrincipal, final String loginContext)
            throws LoginException {
        try {
            if (!initializedLogin) {
                synchronized (this) {
                    if (login == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("JAAS loginContext is: " + loginContext);
                        }
                                                                        login = new Login(loginContext, new SaslClientCallbackHandler(null, "Client"), clientConfig);
                        login.startThreadIfNeeded();
                        initializedLogin = true;
                    }
                }
            }
            return SecurityUtils.createSaslClient(login.getSubject(),
                    servicePrincipal, "zookeeper", "zk-sasl-md5", LOG, "Client");
        } catch (LoginException e) {
                        throw e;
        } catch (Exception e) {
                        LOG.error("Exception while trying to create SASL client: " + e);
            return null;
        }
    }

    public void respondToServer(byte[] serverToken, ClientCnxn cnxn) {
        if (saslClient == null) {
            LOG.error("saslClient is unexpectedly null. Cannot respond to server's SASL message; ignoring.");
            return;
        }

        if (!(saslClient.isComplete())) {
            try {
                saslToken = createSaslToken(serverToken);
                if (saslToken != null) {
                    sendSaslPacket(saslToken, cnxn);
                }
            } catch (SaslException e) {
                LOG.error("SASL authentication failed using login context '" +
                        this.getLoginContext() + "' with exception: {}", e);
                saslState = SaslState.FAILED;
                gotLastPacket = true;
            }
        }

        if (saslClient.isComplete()) {
                                    if ((serverToken == null) && (saslClient.getMechanismName().equals("GSSAPI")))
                gotLastPacket = true;
                        if (!saslClient.getMechanismName().equals("GSSAPI")) {
                gotLastPacket = true;
            }
                                                cnxn.saslCompleted();
        }
    }

    private byte[] createSaslToken() throws SaslException {
        saslState = SaslState.INTERMEDIATE;
        return createSaslToken(saslToken);
    }

    private byte[] createSaslToken(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
                        saslState = SaslState.FAILED;
            throw new SaslException("Error in authenticating with a Zookeeper Quorum member: the quorum member's saslToken is null.");
        }

        Subject subject = login.getSubject();
        if (subject != null) {
            synchronized(login) {
                try {
                    final byte[] retval =
                        Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                                public byte[] run() throws SaslException {
                                    LOG.debug("saslClient.evaluateChallenge(len="+saslToken.length+")");
                                    return saslClient.evaluateChallenge(saslToken);
                                }
                            });
                    return retval;
                }
                catch (PrivilegedActionException e) {
                    String error = "An error: (" + e + ") occurred when evaluating Zookeeper Quorum Member's " +
                      " received SASL token.";
                                                            final String UNKNOWN_SERVER_ERROR_TEXT =
                      "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
                    if (e.toString().contains(UNKNOWN_SERVER_ERROR_TEXT)) {
                        error += " This may be caused by Java's being unable to resolve the Zookeeper Quorum Member's" +
                          " hostname correctly. You may want to try to adding" +
                          " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment.";
                    }
                    error += " Zookeeper Client will go to AUTH_FAILED state.";
                    LOG.error(error);
                    saslState = SaslState.FAILED;
                    throw new SaslException(error, e);
                }
            }
        }
        else {
            throw new SaslException("Cannot make SASL token without subject defined. " +
              "For diagnosis, please look for WARNs and ERRORs in your log related to the Login class.");
        }
    }

    private void sendSaslPacket(byte[] saslToken, ClientCnxn cnxn)
      throws SaslException{
        if (LOG.isDebugEnabled()) {
            LOG.debug("ClientCnxn:sendSaslPacket:length="+saslToken.length);
        }

        GetSASLRequest request = new GetSASLRequest();
        request.setToken(saslToken);
        SetSASLResponse response = new SetSASLResponse();
        ServerSaslResponseCallback cb = new ServerSaslResponseCallback();

        try {
            cnxn.sendPacket(request,response,cb, ZooDefs.OpCode.sasl);
        } catch (IOException e) {
            throw new SaslException("Failed to send SASL packet to server.",
                e);
        }
    }

    private void sendSaslPacket(ClientCnxn cnxn) throws SaslException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ClientCnxn:sendSaslPacket:length="+saslToken.length);
        }
        GetSASLRequest request = new GetSASLRequest();
        request.setToken(createSaslToken());
        SetSASLResponse response = new SetSASLResponse();
        ServerSaslResponseCallback cb = new ServerSaslResponseCallback();
        try {
            cnxn.sendPacket(request,response,cb, ZooDefs.OpCode.sasl);
        } catch (IOException e) {
            throw new SaslException("Failed to send SASL packet to server due " +
              "to IOException:", e);
        }
    }

            public KeeperState getKeeperState() {
        if (saslClient != null) {
            if (saslState == SaslState.FAILED) {
              return KeeperState.AuthFailed;
            }
            if (saslClient.isComplete()) {
                if (saslState == SaslState.INTERMEDIATE) {
                    saslState = SaslState.COMPLETE;
                    return KeeperState.SaslAuthenticated;
                }
            }
        }
                return null;
    }

            public void initialize(ClientCnxn cnxn) throws SaslException {
        if (saslClient == null) {
            saslState = SaslState.FAILED;
            throw new SaslException("saslClient failed to initialize properly: it's null.");
        }
        if (saslState == SaslState.INITIAL) {
            if (saslClient.hasInitialResponse()) {
                sendSaslPacket(cnxn);
            }
            else {
                byte[] emptyToken = new byte[0];
                sendSaslPacket(emptyToken, cnxn);
            }
            saslState = SaslState.INTERMEDIATE;
        }
    }

    public boolean clientTunneledAuthenticationInProgress() {
    	if (!isSASLConfigured) {
    	    return false;
        } 
                                try {
            if ((clientConfig.getJaasConfKey() != null)
                    || ((Configuration.getConfiguration() != null) && (Configuration.getConfiguration()
                            .getAppConfigurationEntry(clientConfig.getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,
                                    ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT)) != null))) {
                                
                                if (!isComplete() && !isFailed()) {
                    return true;
                }

                                if (!gotLastPacket) {
                                                            return true;
                }
            }
                                                return false;
        } catch (SecurityException e) {
                                    if (LOG.isDebugEnabled()) {
                LOG.debug("Could not retrieve login configuration: " + e);
            }
            return false;
        }
    }

    
    public void shutdown() {
        if (null != login) {
            login.shutdown();
        }
    }
}
