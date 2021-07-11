package org.apache.zookeeper.util;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.zookeeper.SaslClientCallbackHandler;
import org.apache.zookeeper.server.auth.KerberosName;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;

public final class SecurityUtils {

    public static final String QUORUM_HOSTNAME_PATTERN = "_HOST";

    
    public static SaslClient createSaslClient(final Subject subject,
            final String servicePrincipal, final String protocol,
            final String serverName, final Logger LOG, final String entity) throws SaslException {
        SaslClient saslClient;
                        if (subject.getPrincipals().isEmpty()) {
                                    LOG.info("{} will use DIGEST-MD5 as SASL mechanism.", entity);
            String[] mechs = { "DIGEST-MD5" };
            String username = (String) (subject.getPublicCredentials()
                    .toArray()[0]);
            String password = (String) (subject.getPrivateCredentials()
                    .toArray()[0]);
                        saslClient = Sasl.createSaslClient(mechs, username, protocol,
                    serverName, null, new SaslClientCallbackHandler(password, entity));
            return saslClient;
        } else {             final Object[] principals = subject.getPrincipals().toArray();
                        final Principal clientPrincipal = (Principal) principals[0];
            boolean usingNativeJgss = Boolean
                    .getBoolean("sun.security.jgss.native");
            if (usingNativeJgss) {
                                                                                                                                                                try {
                    GSSManager manager = GSSManager.getInstance();
                    Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                    GSSCredential cred = manager.createCredential(null,
                            GSSContext.DEFAULT_LIFETIME, krb5Mechanism,
                            GSSCredential.INITIATE_ONLY);
                    subject.getPrivateCredentials().add(cred);
                    LOG.debug("Added private credential to {} principal name: '{}'",
                            entity, clientPrincipal);
                } catch (GSSException ex) {
                    LOG.warn("Cannot add private credential to subject; "
                                    + "authentication at the server may fail", ex);
                }
            }
            final KerberosName clientKerberosName = new KerberosName(
                    clientPrincipal.getName());
                                                String serverRealm = System.getProperty("zookeeper.server.realm",
                    clientKerberosName.getRealm());
            String modifiedServerPrincipal = servicePrincipal;
                        if (!modifiedServerPrincipal.contains("@")) {
                modifiedServerPrincipal = modifiedServerPrincipal + "@" + serverRealm;
            }
            KerberosName serviceKerberosName = new KerberosName(modifiedServerPrincipal);
            final String serviceName = serviceKerberosName.getServiceName();
            final String serviceHostname = serviceKerberosName.getHostName();
            final String clientPrincipalName = clientKerberosName.toString();
            try {
                saslClient = Subject.doAs(subject,
                        new PrivilegedExceptionAction<SaslClient>() {
                            public SaslClient run() throws SaslException {
                                LOG.info("{} will use GSSAPI as SASL mechanism.", entity);
                                String[] mechs = { "GSSAPI" };
                                LOG.debug("creating sasl client: {}={};service={};serviceHostname={}",
                                        new Object[] { entity, clientPrincipalName, serviceName, serviceHostname });
                                SaslClient saslClient = Sasl.createSaslClient(
                                        mechs, clientPrincipalName, serviceName,
                                        serviceHostname, null,
                                        new SaslClientCallbackHandler(null, entity));
                                return saslClient;
                            }
                        });
                return saslClient;
            } catch (Exception e) {
                LOG.error("Exception while trying to create SASL client", e);
                return null;
            }
        }
    }

    
    public static SaslServer createSaslServer(final Subject subject,
            final String protocol, final String serverName,
            final CallbackHandler callbackHandler, final Logger LOG) {
        if (subject != null) {
                                    if (subject.getPrincipals().size() > 0) {
                try {
                    final Object[] principals = subject.getPrincipals()
                            .toArray();
                    final Principal servicePrincipal = (Principal) principals[0];

                                                            final String servicePrincipalNameAndHostname = servicePrincipal
                            .getName();

                    int indexOf = servicePrincipalNameAndHostname.indexOf("/");

                                        final String servicePrincipalName = servicePrincipalNameAndHostname
                            .substring(0, indexOf);

                                                            final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname
                            .substring(indexOf + 1,
                                    servicePrincipalNameAndHostname.length());

                    indexOf = serviceHostnameAndKerbDomain.indexOf("@");
                                        final String serviceHostname = serviceHostnameAndKerbDomain
                            .substring(0, indexOf);

                                                            final String mech = "GSSAPI";

                    LOG.debug("serviceHostname is '" + serviceHostname + "'");
                    LOG.debug("servicePrincipalName is '" + servicePrincipalName
                            + "'");
                    LOG.debug("SASL mechanism(mech) is '" + mech + "'");

                    boolean usingNativeJgss = Boolean
                            .getBoolean("sun.security.jgss.native");
                    if (usingNativeJgss) {
                                                                                                                                                                                                                                                                        try {
                            GSSManager manager = GSSManager.getInstance();
                            Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                            GSSName gssName = manager.createName(
                                    servicePrincipalName + "@"
                                            + serviceHostname,
                                    GSSName.NT_HOSTBASED_SERVICE);
                            GSSCredential cred = manager.createCredential(
                                    gssName, GSSContext.DEFAULT_LIFETIME,
                                    krb5Mechanism, GSSCredential.ACCEPT_ONLY);
                            subject.getPrivateCredentials().add(cred);
                            LOG.debug("Added private credential to service principal name: '{}',"
                                            + " GSSCredential name: {}", servicePrincipalName, cred.getName());
                        } catch (GSSException ex) {
                            LOG.warn("Cannot add private credential to subject; "
                                            + "clients authentication may fail", ex);
                        }
                    }
                    try {
                        return Subject.doAs(subject,
                                new PrivilegedExceptionAction<SaslServer>() {
                                    public SaslServer run() {
                                        try {
                                            SaslServer saslServer;
                                            saslServer = Sasl.createSaslServer(
                                                    mech, servicePrincipalName,
                                                    serviceHostname, null,
                                                    callbackHandler);
                                            return saslServer;
                                        } catch (SaslException e) {
                                            LOG.error("Zookeeper Server failed to create a SaslServer to interact with a client during session initiation: ", e);
                                            return null;
                                        }
                                    }
                                });
                    } catch (PrivilegedActionException e) {
                                                LOG.error("Zookeeper Quorum member experienced a PrivilegedActionException exception while creating a SaslServer using a JAAS principal context:", e);
                    }
                } catch (IndexOutOfBoundsException e) {
                    LOG.error("server principal name/hostname determination error: ", e);
                }
            } else {
                                                                try {
                    SaslServer saslServer = Sasl.createSaslServer("DIGEST-MD5",
                            protocol, serverName, null, callbackHandler);
                    return saslServer;
                } catch (SaslException e) {
                    LOG.error("Zookeeper Quorum member failed to create a SaslServer to interact with a client during session initiation", e);
                }
            }
        }
        return null;
    }

    
    public static String getServerPrincipal(String principalConfig,
            String hostname) {
        String[] components = getComponents(principalConfig);
        if (components == null || components.length != 2
                || !components[1].equals(QUORUM_HOSTNAME_PATTERN)) {
            return principalConfig;
        } else {
            return replacePattern(components, hostname);
        }
    }

    private static String[] getComponents(String principalConfig) {
        if (principalConfig == null)
            return null;
        return principalConfig.split("[/]");
    }

    private static String replacePattern(String[] components, String hostname) {
        return components[0] + "/" + hostname.toLowerCase();
    }
}
