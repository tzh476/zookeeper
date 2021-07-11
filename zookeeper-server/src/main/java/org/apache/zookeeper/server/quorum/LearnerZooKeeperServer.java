package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.DataTreeBean;
import org.apache.zookeeper.server.quorum.LearnerSessionTracker;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServerBean;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;


public abstract class LearnerZooKeeperServer extends QuorumZooKeeperServer {

    
    protected CommitProcessor commitProcessor;
    protected SyncRequestProcessor syncProcessor;

    public LearnerZooKeeperServer(FileTxnSnapLog logFactory, int tickTime,
            int minSessionTimeout, int maxSessionTimeout,
            ZKDatabase zkDb, QuorumPeer self)
        throws IOException
    {
        super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, zkDb, self);
    }

    
    abstract public Learner getLearner();

    
    protected Map<Long, Integer> getTouchSnapshot() {
        if (sessionTracker != null) {
            return ((LearnerSessionTracker) sessionTracker).snapshot();
        }
        Map<Long, Integer> map = Collections.emptyMap();
        return map;
    }

    
    @Override
    public long getServerId() {
        return self.getId();
    }

    @Override
    public void createSessionTracker() {
        sessionTracker = new LearnerSessionTracker(
                this, getZKDatabase().getSessionWithTimeOuts(),
                this.tickTime, self.getId(), self.areLocalSessionsEnabled(),
                getZooKeeperServerListener());
    }

    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
            int sessionTimeout) throws IOException {
        if (upgradeableSessionTracker.isLocalSession(sessionId)) {
            super.revalidateSession(cnxn, sessionId, sessionTimeout);
        } else {
            getLearner().validateSession(cnxn, sessionId, sessionTimeout);
        }
    }

    @Override
    protected void registerJMX() {
                try {
            jmxDataTreeBean = new DataTreeBean(getZKDatabase().getDataTree());
            MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxDataTreeBean = null;
        }
    }

    public void registerJMX(ZooKeeperServerBean serverBean,
            LocalPeerBean localPeerBean)
    {
                if (self.jmxLeaderElectionBean != null) {
            try {
                MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }

        try {
            jmxServerBean = serverBean;
            MBeanRegistry.getInstance().register(serverBean, localPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    @Override
    protected void unregisterJMX() {
                try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxDataTreeBean = null;
    }

    protected void unregisterJMX(Learner peer) {
                try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
    }

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        LOG.info("Shutting down");
        try {
            super.shutdown();
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }
        try {
            if (syncProcessor != null) {
                syncProcessor.shutdown();
            }
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception in syncprocessor shutdown",
                    e);
        }
    }
}
