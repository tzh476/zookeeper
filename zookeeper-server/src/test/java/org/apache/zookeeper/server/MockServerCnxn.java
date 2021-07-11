package org.apache.zookeeper.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.proto.ReplyHeader;

public class MockServerCnxn extends ServerCnxn {
    public Certificate[] clientChain;
    public boolean secure;

    @Override
    int getSessionTimeout() {
        return 0;
    }

    @Override
    void close() {
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag)
            throws IOException {
    }

    @Override
    void sendCloseSession() {
    }

    @Override
    public void process(WatchedEvent event) {
    }

    @Override
    public long getSessionId() {
        return 0;
    }

    @Override
    void setSessionId(long sessionId) {
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        return clientChain;
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        clientChain = chain;
    }

    @Override
    void sendBuffer(ByteBuffer closeConn) {
    }

    @Override
    void enableRecv() {
    }

    @Override
    void disableRecv() {
    }

    @Override
    void setSessionTimeout(int sessionTimeout) {
    }

    @Override
    protected ServerStats serverStats() {
        return null;
    }

    @Override
    public long getOutstandingRequests() {
        return 0;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return null;
    }

    @Override
    public int getInterestOps() {
        return 0;
    }
}