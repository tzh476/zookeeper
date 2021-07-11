package org.apache.zookeeper;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jute.Record;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;

public class TestableZooKeeper extends ZooKeeperAdmin {

    public TestableZooKeeper(String host, int sessionTimeout,
            Watcher watcher) throws IOException {
        super(host, sessionTimeout, watcher);
    }

    class TestableClientCnxn extends ClientCnxn {
        TestableClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly)
                throws IOException {
            super(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher,
                clientCnxnSocket, 0, new byte[16], canBeReadOnly);
        }

        void setXid(int newXid) {
            xid = newXid;
        }

        int checkXid() {
            return xid;
        }
    }

    protected ClientCnxn createConnection(String chrootPath,
            HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
            boolean canBeReadOnly) throws IOException {
        return new TestableClientCnxn(chrootPath, hostProvider, sessionTimeout, this,
                watcher, clientCnxnSocket, canBeReadOnly);
    }

    public void setXid(int xid) {
        ((TestableClientCnxn)cnxn).setXid(xid);
    }

    public int checkXid() {
        return ((TestableClientCnxn)cnxn).checkXid();
    }
    
    @Override
    public List<String> getChildWatches() {
        return super.getChildWatches();
    }


    @Override
    public List<String> getDataWatches() {
        return super.getDataWatches();
    }


    @Override
    public List<String> getExistWatches() {
        return super.getExistWatches();
    }

    
    public void testableConnloss() throws IOException {
        synchronized(cnxn) {
            cnxn.sendThread.testableCloseSocket();
        }
    }

    
    public boolean pauseCnxn(final long ms) {
        final CountDownLatch initiatedPause = new CountDownLatch(1);
        new Thread() {
            public void run() {
                synchronized(cnxn) {
                    try {
                        try {
                            cnxn.sendThread.testableCloseSocket();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            initiatedPause.countDown();
                        }
                        Thread.sleep(ms);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }.start();

        try {
            return initiatedPause.await(ms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public SocketAddress testableLocalSocketAddress() {
        return super.testableLocalSocketAddress();
    }

    public SocketAddress testableRemoteSocketAddress() {
        return super.testableRemoteSocketAddress();
    }

    
    public long testableLastZxid() {
        return cnxn.getLastZxid();
    }

    public ReplyHeader submitRequest(RequestHeader h, Record request,
            Record response, WatchRegistration watchRegistration) throws InterruptedException {
        return cnxn.submitRequest(h, request, response, watchRegistration);
    }

    
    public void disconnect() {
        cnxn.disconnect();
    }
}
