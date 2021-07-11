package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerBean;


public class FollowerBean extends ZooKeeperServerBean implements FollowerMXBean {
    private final Follower follower;

    public FollowerBean(Follower follower, ZooKeeperServer zks) {
        super(zks);
        this.follower = follower;
    }
    
    public String getName() {
        return "Follower";
    }

    public String getQuorumAddress() {
        return follower.sock.toString();
    }
    
    public String getLastQueuedZxid() {
        return "0x" + Long.toHexString(follower.getLastQueued());
    }
    
    public int getPendingRevalidationCount() {
        return follower.getPendingRevalidationsCount();
    }

    @Override
    public long getElectionTimeTaken() {
        return follower.self.getElectionTimeTaken();
    }
}
