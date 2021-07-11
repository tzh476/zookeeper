package org.apache.zookeeper.server;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.io.IOException;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;

public class MockNIOServerCnxn extends NIOServerCnxn {

    public MockNIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
                         SelectionKey sk, NIOServerCnxnFactory factory,
                         SelectorThread selectorThread) throws IOException {
        super(zk, sock, sk, factory, selectorThread);
    }

    
    public void doIO(SelectionKey k) throws InterruptedException {
        super.doIO(k);
    }

    @Override
    protected boolean isSocketOpen() {
        return true;
    }
}
