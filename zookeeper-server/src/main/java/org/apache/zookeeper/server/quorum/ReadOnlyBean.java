package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerBean;


public class ReadOnlyBean extends ZooKeeperServerBean {

    public ReadOnlyBean(ZooKeeperServer zks) {
        super(zks);
    }

    public String getName() {
        return "ReadOnlyServer";
    }

}
