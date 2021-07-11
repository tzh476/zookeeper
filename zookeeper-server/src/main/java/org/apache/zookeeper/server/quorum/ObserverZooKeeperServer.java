package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;


public class ObserverZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(ObserverZooKeeperServer.class);        
    
    
    
    private boolean syncRequestProcessorEnabled = this.self.getSyncEnabled();
    
    
    ConcurrentLinkedQueue<Request> pendingSyncs = 
        new ConcurrentLinkedQueue<Request>();

    ObserverZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, zkDb, self);
        LOG.info("syncEnabled =" + syncRequestProcessorEnabled);
    }
    
    public Observer getObserver() {
        return self.observer;
    }
    
    @Override
    public Learner getLearner() {
        return self.observer;
    }       
    
    
    public void commitRequest(Request request) {     
        if (syncRequestProcessorEnabled) {
                        syncProcessor.processRequest(request);
        }
        commitProcessor.commit(request);        
    }
    
    
    @Override
    protected void setupRequestProcessors() {      
                                RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor,
                Long.toString(getServerId()), true,
                getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new ObserverRequestProcessor(this, commitProcessor);
        ((ObserverRequestProcessor) firstProcessor).start();

        
        if (syncRequestProcessorEnabled) {
            syncProcessor = new SyncRequestProcessor(this, null);
            syncProcessor.start();
        }
    }

    
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){
            LOG.warn("Not expecting a sync.");
            return;
        }
                
        Request r = pendingSyncs.remove();
        commitProcessor.commit(r);
    }
    
    @Override
    public String getState() {
        return "observer";
    };    

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        super.shutdown();
        if (syncRequestProcessorEnabled && syncProcessor != null) {
            syncProcessor.shutdown();
        }
    }
}
