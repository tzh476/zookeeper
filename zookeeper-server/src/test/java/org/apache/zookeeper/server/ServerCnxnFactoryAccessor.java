package org.apache.zookeeper.server;

public class ServerCnxnFactoryAccessor {
    public static ZooKeeperServer getZkServer(ServerCnxnFactory fac) {
	return fac.zkServer;
    }
}
