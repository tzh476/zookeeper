package org.apache.zookeeper;


import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.HostProvider;
import org.junit.Assert;
import org.junit.Test;

public class ClientReconnectTest extends ZKTestCase {
    private SocketChannel sc;
    private CountDownLatch countDownLatch = new CountDownLatch(3);
    
    class MockCnxn extends ClientCnxnSocketNIO {
        MockCnxn() throws IOException {
            super(new ZKClientConfig());
        }

        @Override
        void registerAndConnect(SocketChannel sock, InetSocketAddress addr) throws
        IOException {
            countDownLatch.countDown();
            throw new IOException("failed to register");
        }

        @Override
        SocketChannel createSock() {
            return sc;
        }
    }

    @Test
    public void testClientReconnect() throws IOException, InterruptedException {
        HostProvider hostProvider = mock(HostProvider.class);
        when(hostProvider.size()).thenReturn(1);
        InetSocketAddress inaddr = new InetSocketAddress("127.0.0.1", 1111);
        when(hostProvider.next(anyLong())).thenReturn(inaddr);
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getClientConfig()).thenReturn(new ZKClientConfig());
        sc =  SocketChannel.open();

        ClientCnxnSocketNIO nioCnxn = new MockCnxn();
        ClientWatchManager watcher = mock(ClientWatchManager.class);
        ClientCnxn clientCnxn = new ClientCnxn(
                "tmp", hostProvider, 5000,
                zk, watcher, nioCnxn, false);
        clientCnxn.start();
        countDownLatch.await(5000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(countDownLatch.getCount() == 0);
        clientCnxn.close();
    }
}
