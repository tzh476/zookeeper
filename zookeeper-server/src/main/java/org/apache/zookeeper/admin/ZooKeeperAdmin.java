package org.apache.zookeeper.admin;

import java.io.IOException;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("try")
@InterfaceAudience.Public
public class ZooKeeperAdmin extends ZooKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperAdmin.class);

    
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException {
        super(connectString, sessionTimeout, watcher);
    }

    
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher,
            ZKClientConfig conf) throws IOException {
        super(connectString, sessionTimeout, watcher, conf);
    }

    
    public ZooKeeperAdmin(String connectString, int sessionTimeout, Watcher watcher,
                     boolean canBeReadOnly) throws IOException {
        super(connectString, sessionTimeout, watcher, canBeReadOnly);
    }

    
    public byte[] reconfigure(String joiningServers, String leavingServers,
                              String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    
    public byte[] reconfigure(List<String> joiningServers, List<String> leavingServers,
                              List<String> newMembers, long fromConfig,
                              Stat stat) throws KeeperException, InterruptedException {
        return internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    
    public void reconfigure(String joiningServers, String leavingServers,
                            String newMembers, long fromConfig, DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    
    public void reconfigure(List<String> joiningServers,
                            List<String> leavingServers, List<String> newMembers, long fromConfig,
                            DataCallback cb, Object ctx) {
        internalReconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }

    
    @Override
    public String toString() {
        return super.toString();
    }
}
