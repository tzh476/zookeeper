package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.jmx.ZKMBeanInfo;


public class RemotePeerBean implements RemotePeerMXBean,ZKMBeanInfo {
    private QuorumPeer.QuorumServer peer;
    private final QuorumPeer localPeer;

    public RemotePeerBean(QuorumPeer localPeer, QuorumPeer.QuorumServer peer){
        this.peer=peer;
        this.localPeer = localPeer;
    }

    public void setQuorumServer(QuorumPeer.QuorumServer peer) {
        this.peer = peer;
    }

    public String getName() {
        return "replica."+peer.id;
    }
    public boolean isHidden() {
        return false;
    }

    public String getQuorumAddress() {
        return peer.addr.getHostString()+":"+peer.addr.getPort();
    }

    public String getElectionAddress() {
        return peer.electionAddr.getHostString() + ":" + peer.electionAddr.getPort();
    }

    public String getClientAddress() {
        if (null == peer.clientAddr) {
            return "";
        }
        return peer.clientAddr.getHostString() + ":"
                + peer.clientAddr.getPort();
    }

    public String getLearnerType() {
        return peer.type.toString();
    }

    @Override
    public boolean isLeader() {
        return localPeer.isLeader(peer.getId());
    }
    
}
