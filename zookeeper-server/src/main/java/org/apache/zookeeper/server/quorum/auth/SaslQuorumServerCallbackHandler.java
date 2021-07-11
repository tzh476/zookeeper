package org.apache.zookeeper.server.quorum.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SaslQuorumServerCallbackHandler implements CallbackHandler {
    private static final String USER_PREFIX = "user_";
    private static final Logger LOG = LoggerFactory.getLogger(SaslQuorumServerCallbackHandler.class);

    private String userName;
    private final Map<String,String> credentials = new HashMap<String,String>();
    private final Set<String> authzHosts;

    public SaslQuorumServerCallbackHandler(Configuration configuration,
            String serverSection, Set<String> authzHosts) throws IOException {
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

                this.authzHosts = authzHosts;
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
            LOG.warn("User '{}' not found in list of DIGEST-MD5 authenticateable users.",
                    nc.getDefaultName());
            return;
        }
        nc.setName(nc.getDefaultName());
        userName = nc.getDefaultName();
    }

    private void handlePasswordCallback(PasswordCallback pc) {
        if (credentials.containsKey(userName) ) {
            pc.setPassword(credentials.get(userName).toCharArray());
        } else {
            LOG.warn("No password found for user: {}", userName);
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.debug("QuorumLearner supplied realm: {}", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();

        boolean authzFlag = false;
                authzFlag = authenticationID.equals(authorizationID);

                                if (authzFlag) {
            String[] components = authorizationID.split("[/@]");
            if (components.length == 3) {
                authzFlag = authzHosts.contains(components[1]);
            }
            if (!authzFlag) {
                LOG.error("SASL authorization completed, {} is not authorized to connect",
                        components[1]);
            }
        }

                ac.setAuthorized(authzFlag);
        if (ac.isAuthorized()) {
            ac.setAuthorizedID(authorizationID);
            LOG.info("Successfully authenticated learner: authenticationID={};  authorizationID={}.",
                    authenticationID, authorizationID);
        }
        LOG.debug("SASL authorization completed, authorized flag set to {}", ac.isAuthorized());
    }
}
