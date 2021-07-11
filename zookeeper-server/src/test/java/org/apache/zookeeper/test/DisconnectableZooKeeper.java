package org.apache.zookeeper.test;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DisconnectableZooKeeper extends ZooKeeper {
    public DisconnectableZooKeeper(String host, int sessionTimeout, Watcher watcher)
        throws IOException
    {
        super(host, sessionTimeout, watcher);
    }
    
    public DisconnectableZooKeeper(String host, int sessionTimeout, Watcher watcher,
        long sessionId, byte[] sessionPasswd)
        throws IOException
    {
        super(host, sessionTimeout, watcher, sessionId, sessionPasswd);
    }

    
    public void disconnect() throws IOException {
        cnxn.disconnect();
    }

    
    public void dontReconnect() throws Exception {
        java.lang.reflect.Field f = cnxn.getClass().getDeclaredField("closing");
        f.setAccessible(true);
        f.setBoolean(cnxn, true);
    }
}
