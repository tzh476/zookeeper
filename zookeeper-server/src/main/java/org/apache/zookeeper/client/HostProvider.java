package org.apache.zookeeper.client;

import org.apache.yetus.audience.InterfaceAudience;

import java.net.InetSocketAddress;
import java.util.Collection;


@InterfaceAudience.Public
public interface HostProvider {
    public int size();

    
    public InetSocketAddress next(long spinDelay);

    
    public void onConnected();

    
    boolean updateServerList(Collection<InetSocketAddress> serverAddresses,
        InetSocketAddress currentHost);
}
