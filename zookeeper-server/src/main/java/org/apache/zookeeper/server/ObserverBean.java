package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.Observer;
import org.apache.zookeeper.server.quorum.ObserverMXBean;


public class ObserverBean extends ZooKeeperServerBean implements ObserverMXBean{

    private Observer observer;
    
    public ObserverBean(Observer observer, ZooKeeperServer zks) {
        super(zks);        
        this.observer = observer;
    }

    public String getName() {
        return "Observer";
    }

    public int getPendingRevalidationCount() {
       return this.observer.getPendingRevalidationsCount(); 
    }

    public String getQuorumAddress() {
        return observer.getSocket().toString();
    }

}
