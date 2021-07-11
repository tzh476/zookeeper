package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.quorum.QuorumPeer;

public class QuorumBean implements QuorumMXBean, ZKMBeanInfo {
    private final QuorumPeer peer;
    private final String name;
    
    public QuorumBean(QuorumPeer peer){
        this.peer = peer;
        name = "ReplicatedServer_id" + peer.getId();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public int getQuorumSize() {
        return peer.getQuorumSize();
    }

    @Override
    public boolean isSslQuorum() {
        return peer.isSslQuorum();
    }

    @Override
    public boolean isPortUnification() {
        return peer.shouldUsePortUnification();
    }
}
