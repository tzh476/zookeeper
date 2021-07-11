package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.server.ZooKeeperServer;


public class DummyAdminServer implements AdminServer {
    @Override
    public void start() throws AdminServerException {}

    @Override
    public void shutdown() throws AdminServerException {}

    @Override
    public void setZooKeeperServer(ZooKeeperServer zkServer) {}
}
