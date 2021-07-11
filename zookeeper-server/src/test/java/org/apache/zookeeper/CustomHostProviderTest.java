package org.apache.zookeeper;

import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomHostProviderTest extends ZKTestCase implements Watcher {
    private AtomicInteger counter = new AtomicInteger(3);

    private class SpecialHostProvider implements HostProvider {
                        @Override
        public int size() {
            return 1;
        }
        @Override
        public InetSocketAddress next(long spinDelay) {
            return new InetSocketAddress("127.0.0.1", 2181);
        }
        @Override
        public void onConnected() {
        }
        @Override
        public boolean updateServerList(Collection<InetSocketAddress>
                serverAddresses, InetSocketAddress currentHost) {
            counter.decrementAndGet();
            return false;
        }
    }
    @Override
    public void process(WatchedEvent event) {
    }

    @Test
    public void testZooKeeperWithCustomHostProvider() throws IOException,
            InterruptedException {
        final int CLIENT_PORT = PortAssignment.unique();
        final HostProvider specialHostProvider = new SpecialHostProvider();
        int expectedCounter = 3;
        counter.set(expectedCounter);

        ZooKeeper zkDefaults = new ZooKeeper("127.0.0.1:" + CLIENT_PORT,
                ClientBase.CONNECTION_TIMEOUT, this, false);

        ZooKeeper zkSpecial = new ZooKeeper("127.0.0.1:" + CLIENT_PORT,
                ClientBase.CONNECTION_TIMEOUT, this, false, specialHostProvider);

        Assert.assertTrue(counter.get() == expectedCounter);
        zkDefaults.updateServerList("127.0.0.1:" + PortAssignment.unique());
        Assert.assertTrue(counter.get() == expectedCounter);

        zkSpecial.updateServerList("127.0.0.1:" + PortAssignment.unique());
        expectedCounter--;
        Assert.assertTrue(counter.get() == expectedCounter);
    }
}
