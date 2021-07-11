package org.apache.zookeeper.server.admin;

import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.server.ZooKeeperServer;


public interface Command {
    
    Set<String> getNames();

    
    String getPrimaryName();

    
    String getDoc();

    
    CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs);
}
