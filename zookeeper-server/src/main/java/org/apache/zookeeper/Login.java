package org.apache.zookeeper;



import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.CallbackHandler;

import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.Subject;

import java.util.Date;
import java.util.Random;
import java.util.Set;

public class Login {
    private static final String KINIT_COMMAND_DEFAULT = "/usr/bin/kinit";
    private static final Logger LOG = LoggerFactory.getLogger(Login.class);
    public CallbackHandler callbackHandler;

                private static final float TICKET_RENEW_WINDOW = 0.80f;

    
    private static final float TICKET_RENEW_JITTER = 0.05f;

                private static final long MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;

    private Subject subject = null;
    private Thread t = null;
    private boolean isKrbTicket = false;
    private boolean isUsingTicketCache = false;

    
    private static Random rng = new Random();

    private LoginContext login = null;
    private String loginContextName = null;
    private String principal = null;

        private long lastLogin = Time.currentElapsedTime() - MIN_TIME_BEFORE_RELOGIN;
    private final ZKConfig zkConfig;

    
    public Login(final String loginContextName, CallbackHandler callbackHandler, final ZKConfig zkConfig)
            throws LoginException {
        this.zkConfig=zkConfig;
        this.callbackHandler = callbackHandler;
        login = login(loginContextName);
        this.loginContextName = loginContextName;
        subject = login.getSubject();
        isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
        AppConfigurationEntry entries[] = Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
        for (AppConfigurationEntry entry: entries) {
                        if (entry.getOptions().get("useTicketCache") != null) {
                String val = (String)entry.getOptions().get("useTicketCache");
                if (val.equals("true")) {
                    isUsingTicketCache = true;
                }
            }
            if (entry.getOptions().get("principal") != null) {
                principal = (String)entry.getOptions().get("principal");
            }
            break;
        }

        if (!isKrbTicket) {
                        return;
        }

                                        t = new Thread(new Runnable() {
            public void run() {
                LOG.info("TGT refresh thread started.");
                while (true) {                      KerberosTicket tgt = getTGT();
                    long now = Time.currentWallTime();
                    long nextRefresh;
                    Date nextRefreshDate;
                    if (tgt == null) {
                        nextRefresh = now + MIN_TIME_BEFORE_RELOGIN;
                        nextRefreshDate = new Date(nextRefresh);
                        LOG.warn("No TGT found: will try again at {}", nextRefreshDate);
                    } else {
                        nextRefresh = getRefreshTime(tgt);
                        long expiry = tgt.getEndTime().getTime();
                        Date expiryDate = new Date(expiry);
                        if ((isUsingTicketCache) && (tgt.getEndTime().equals(tgt.getRenewTill()))) {
                            Object[] logPayload = {expiryDate, principal, principal};
                            LOG.error("The TGT cannot be renewed beyond the next expiry date: {}." +
                                    "This process will not be able to authenticate new SASL connections after that " +
                                    "time (for example, it will not be authenticate a new connection with a Zookeeper " +
                                    "Quorum member).  Ask your system administrator to either increase the " +
                                    "'renew until' time by doing : 'modprinc -maxrenewlife {}' within " +
                                    "kadmin, or instead, to generate a keytab for {}. Because the TGT's " +
                                    "expiry cannot be further extended by refreshing, exiting refresh thread now.", logPayload);
                            return;
                        }
                                                                                                                        if ((nextRefresh > expiry) ||
                                ((now + MIN_TIME_BEFORE_RELOGIN) > expiry)) {
                                                        nextRefresh = now;
                        } else {
                            if (nextRefresh < (now + MIN_TIME_BEFORE_RELOGIN)) {
                                                                Date until = new Date(nextRefresh);
                                Date newuntil = new Date(now + MIN_TIME_BEFORE_RELOGIN);
                                Object[] logPayload = {until, newuntil, (MIN_TIME_BEFORE_RELOGIN / 1000)};
                                LOG.warn("TGT refresh thread time adjusted from : {} to : {} since "
                                        + "the former is sooner than the minimum refresh interval ("
                                        + "{} seconds) from now.", logPayload);
                            }
                            nextRefresh = Math.max(nextRefresh, now + MIN_TIME_BEFORE_RELOGIN);
                        }
                        nextRefreshDate = new Date(nextRefresh);
                        if (nextRefresh > expiry) {
                            Object[] logPayload = {nextRefreshDate, expiryDate};
                            LOG.error("next refresh: {} is later than expiry {}."
                                    + " This may indicate a clock skew problem. Check that this host and the KDC's "
                                    + "hosts' clocks are in sync. Exiting refresh thread.", logPayload);
                            return;
                        }
                    }
                    if (now == nextRefresh) {
                        LOG.info("refreshing now because expiry is before next scheduled refresh time.");
                    } else if (now < nextRefresh) {
                        Date until = new Date(nextRefresh);
                        LOG.info("TGT refresh sleeping until: {}", until.toString());
                        try {
                            Thread.sleep(nextRefresh - now);
                        } catch (InterruptedException ie) {
                            LOG.warn("TGT renewal thread has been interrupted and will exit.");
                            break;
                        }
                    }
                    else {
                        LOG.error("nextRefresh:{} is in the past: exiting refresh thread. Check"
                                + " clock sync between this host and KDC - (KDC's clock is likely ahead of this host)."
                                + " Manual intervention will be required for this client to successfully authenticate."
                                + " Exiting refresh thread.", nextRefreshDate);
                        break;
                    }
                    if (isUsingTicketCache) {
                        String cmd = zkConfig.getProperty(ZKConfig.KINIT_COMMAND, KINIT_COMMAND_DEFAULT);
                        String kinitArgs = "-R";
                        int retry = 1;
                        while (retry >= 0) {
                            try {
                                LOG.debug("running ticket cache refresh command: {} {}", cmd, kinitArgs);
                                Shell.execCommand(cmd, kinitArgs);
                                break;
                            } catch (Exception e) {
                                if (retry > 0) {
                                    --retry;
                                                                        try {
                                        Thread.sleep(10 * 1000);
                                    } catch (InterruptedException ie) {
                                        LOG.error("Interrupted while renewing TGT, exiting Login thread");
                                        return;
                                    }
                                } else {
                                    Object[] logPayload = {cmd, kinitArgs, e.toString(), e};
                                    LOG.warn("Could not renew TGT due to problem running shell command: '{}"
                                            + " {}'; exception was:{}. Exiting refresh thread.", logPayload);
                                    return;
                                }
                            }
                        }
                    }
                    try {
                        int retry = 1;
                        while (retry >= 0) {
                            try {
                                reLogin();
                                break;
                            } catch (LoginException le) {
                                if (retry > 0) {
                                    --retry;
                                                                        try {
                                        Thread.sleep(10 * 1000);
                                    } catch (InterruptedException e) {
                                        LOG.error("Interrupted during login retry after LoginException:", le);
                                        throw le;
                                    }
                                } else {
                                    LOG.error("Could not refresh TGT for principal: {}.", principal, le);
                                }
                            }
                        }
                    } catch (LoginException le) {
                        LOG.error("Failed to refresh TGT: refresh thread exiting now.",le);
                        break;
                    }
                }
            }
        });
        t.setDaemon(true);
    }

    public void startThreadIfNeeded() {
                if (t != null) {
            t.start();
        }
    }

    public void shutdown() {
        if ((t != null) && (t.isAlive())) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.warn("error while waiting for Login thread to shutdown: ", e);
            }
        }
    }

    public Subject getSubject() {
        return subject;
    }

    public String getLoginContextName() {
        return loginContextName;
    }

    private synchronized LoginContext login(final String loginContextName) throws LoginException {
        if (loginContextName == null) {
            throw new LoginException("loginContext name (JAAS file section header) was null. " +
                    "Please check your java.security.login.auth.config (=" +
                    System.getProperty("java.security.login.auth.config") +
                    ") and your " + getLoginContextMessage());
        }
        LoginContext loginContext = new LoginContext(loginContextName,callbackHandler);
        loginContext.login();
        LOG.info("{} successfully logged in.", loginContextName);
        return loginContext;
    }

    private String getLoginContextMessage() {
        if (zkConfig instanceof ZKClientConfig) {
            return ZKClientConfig.LOGIN_CONTEXT_NAME_KEY + "(=" + zkConfig.getProperty(
                    ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT) + ")";
        } else {
            return ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY + "(=" + System.getProperty(
                    ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY, ZooKeeperSaslServer.DEFAULT_LOGIN_CONTEXT_NAME) + ")";
        }
    }

        private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long expires = tgt.getEndTime().getTime();
        LOG.info("TGT valid starting at:        {}", tgt.getStartTime().toString());
        LOG.info("TGT expires:                  {}", tgt.getEndTime().toString());
        long proposedRefresh = start + (long) ((expires - start) *
                (TICKET_RENEW_WINDOW + (TICKET_RENEW_JITTER * rng.nextDouble())));
        if (proposedRefresh > expires) {
                        return Time.currentWallTime();
        }
        else {
            return proposedRefresh;
        }
    }

    private synchronized KerberosTicket getTGT() {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for(KerberosTicket ticket: tickets) {
            KerberosPrincipal server = ticket.getServer();
            if (server.getName().equals("krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
                LOG.debug("Client principal is \"" + ticket.getClient().getName() + "\".");
                LOG.debug("Server principal is \"" + ticket.getServer().getName() + "\".");
                return ticket;
            }
        }
        return null;
    }

    private boolean hasSufficientTimeElapsed() {
        long now = Time.currentElapsedTime();
        if (now - getLastLogin() < MIN_TIME_BEFORE_RELOGIN ) {
            LOG.warn("Not attempting to re-login since the last re-login was "
                    + "attempted less than {} seconds before.",
                    (MIN_TIME_BEFORE_RELOGIN / 1000));
            return false;
        }
                setLastLogin(now);
        return true;
    }

    
    private LoginContext getLogin() {
        return login;
    }

    
    private void setLogin(LoginContext login) {
        this.login = login;
    }

    
    private void setLastLogin(long time) {
        lastLogin = time;
    }

    
    private long getLastLogin() {
        return lastLogin;
    }

    
        private synchronized void reLogin()
            throws LoginException {
        if (!isKrbTicket) {
            return;
        }
        LoginContext login = getLogin();
        if (login  == null) {
            throw new LoginException("login must be done first");
        }
        if (!hasSufficientTimeElapsed()) {
            return;
        }
        LOG.info("Initiating logout for {}", principal);
        synchronized (Login.class) {
                                                login.logout();
                                    login = new LoginContext(loginContextName, getSubject());
            LOG.info("Initiating re-login for {}", principal);
            login.login();
            setLogin(login);
        }
    }
}
