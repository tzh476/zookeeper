package org.apache.zookeeper.server.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.zookeeper.server.ZooKeeperSaslServer;

public class SaslServerCallbackHandler implements CallbackHandler {
    private static final String USER_PREFIX = "user_";
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerCallbackHandler.class);
    private static final String SYSPROP_SUPER_PASSWORD = "zookeeper.SASLAuthenticationProvider.superPassword";
    private static final String SYSPROP_REMOVE_HOST = "zookeeper.kerberos.removeHostFromPrincipal";
    private static final String SYSPROP_REMOVE_REALM = "zookeeper.kerberos.removeRealmFromPrincipal";

    private String userName;
    private final Map<String,String> credentials = new HashMap<String,String>();

    public SaslServerCallbackHandler(Configuration configuration)
            throws IOException {
        String serverSection = System.getProperty(
                ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
                ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME);
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(serverSection);

        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + serverSection + "' entry in this configuration: Server cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
        credentials.clear();
        for(AppConfigurationEntry entry: configurationEntries) {
            Map<String,?> options = entry.getOptions();
                                    for(Map.Entry<String, ?> pair : options.entrySet()) {
                String key = pair.getKey();
                if (key.startsWith(USER_PREFIX)) {
                    String userName = key.substring(USER_PREFIX.length());
                    credentials.put(userName,(String)pair.getValue());
                }
            }
        }
    }

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                handleNameCallback((NameCallback) callback);
            } else if (callback instanceof PasswordCallback) {
                handlePasswordCallback((PasswordCallback) callback);
            } else if (callback instanceof RealmCallback) {
                handleRealmCallback((RealmCallback) callback);
            } else if (callback instanceof AuthorizeCallback) {
                handleAuthorizeCallback((AuthorizeCallback) callback);
            }
        }
    }

    private void handleNameCallback(NameCallback nc) {
                if (credentials.get(nc.getDefaultName()) == null) {
            LOG.warn("User '" + nc.getDefaultName() + "' not found in list of DIGEST-MD5 authenticateable users.");
            return;
        }
        nc.setName(nc.getDefaultName());
        userName = nc.getDefaultName();
    }

    private void handlePasswordCallback(PasswordCallback pc) {
        if ("super".equals(this.userName) && System.getProperty(SYSPROP_SUPER_PASSWORD) != null) {
                        pc.setPassword(System.getProperty(SYSPROP_SUPER_PASSWORD).toCharArray());
        } else if (credentials.containsKey(userName) ) {
            pc.setPassword(credentials.get(userName).toCharArray());
        } else {
            LOG.warn("No password found for user: " + userName);
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.debug("client supplied realm: " + rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();

        LOG.info("Successfully authenticated client: authenticationID=" + authenticationID
                + ";  authorizationID=" + authorizationID + ".");
        ac.setAuthorized(true);

                                KerberosName kerberosName = new KerberosName(authenticationID);
        try {
            StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
            if (shouldAppendHost(kerberosName)) {
                userNameBuilder.append("/").append(kerberosName.getHostName());
            }
            if (shouldAppendRealm(kerberosName)) {
                userNameBuilder.append("@").append(kerberosName.getRealm());
            }
            LOG.info("Setting authorizedID: " + userNameBuilder);
            ac.setAuthorizedID(userNameBuilder.toString());
        } catch (IOException e) {
            LOG.error("Failed to set name based on Kerberos authentication rules.", e);
        }
    }

    private boolean shouldAppendRealm(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_REALM) && kerberosName.getRealm() != null;
    }

    private boolean shouldAppendHost(KerberosName kerberosName) {
        return !isSystemPropertyTrue(SYSPROP_REMOVE_HOST) && kerberosName.getHostName() != null;
    }

    private boolean isSystemPropertyTrue(String propertyName) {
        return "true".equals(System.getProperty(propertyName));
    }
}
