package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class MockSelectorThread extends NIOServerCnxnFactory.SelectorThread {
    public MockSelectorThread(NIOServerCnxnFactory fact) throws IOException {
        fact.super(0);
    }

    public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
        return super.addInterestOpsUpdateRequest(sk);
    }
}
