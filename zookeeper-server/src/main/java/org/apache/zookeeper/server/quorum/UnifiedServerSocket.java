package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLSocket;

import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslHandler;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnifiedServerSocket extends ServerSocket {
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedServerSocket.class);

    private X509Util x509Util;
    private final boolean allowInsecureConnection;

    
    public UnifiedServerSocket(X509Util x509Util, boolean allowInsecureConnection) throws IOException {
        super();
        this.x509Util = x509Util;
        this.allowInsecureConnection = allowInsecureConnection;
    }

    
    public UnifiedServerSocket(X509Util x509Util, boolean allowInsecureConnection, int port) throws IOException {
        super(port);
        this.x509Util = x509Util;
        this.allowInsecureConnection = allowInsecureConnection;
    }

    
    public UnifiedServerSocket(X509Util x509Util,
                               boolean allowInsecureConnection,
                               int port,
                               int backlog) throws IOException {
        super(port, backlog);
        this.x509Util = x509Util;
        this.allowInsecureConnection = allowInsecureConnection;
    }

    
    public UnifiedServerSocket(X509Util x509Util,
                               boolean allowInsecureConnection,
                               int port,
                               int backlog,
                               InetAddress bindAddr) throws IOException {
        super(port, backlog, bindAddr);
        this.x509Util = x509Util;
        this.allowInsecureConnection = allowInsecureConnection;
    }

    @Override
    public Socket accept() throws IOException {
        if (isClosed()) {
            throw new SocketException("Socket is closed");
        }
        if (!isBound()) {
            throw new SocketException("Socket is not bound yet");
        }
        final PrependableSocket prependableSocket = new PrependableSocket(null);
        implAccept(prependableSocket);
        return new UnifiedSocket(x509Util, allowInsecureConnection, prependableSocket);
    }

    
    public static class UnifiedSocket extends Socket {
        private enum Mode {
            UNKNOWN,
            PLAINTEXT,
            TLS
        }

        private final X509Util x509Util;
        private final boolean allowInsecureConnection;
        private PrependableSocket prependableSocket;
        private SSLSocket sslSocket;
        private Mode mode;

        
        private UnifiedSocket(X509Util x509Util, boolean allowInsecureConnection, PrependableSocket prependableSocket) {
            this.x509Util = x509Util;
            this.allowInsecureConnection = allowInsecureConnection;
            this.prependableSocket = prependableSocket;
            this.sslSocket = null;
            this.mode = Mode.UNKNOWN;
        }

        
        public boolean isSecureSocket() {
            return mode == Mode.TLS;
        }

        
        public boolean isPlaintextSocket() {
            return mode == Mode.PLAINTEXT;
        }

        
        public boolean isModeKnown() {
            return mode != Mode.UNKNOWN;
        }

        
        private void detectMode() throws IOException {
            byte[] litmus = new byte[5];
            int oldTimeout = -1;
            int bytesRead = 0;
            int newTimeout = x509Util.getSslHandshakeTimeoutMillis();
            try {
                oldTimeout = prependableSocket.getSoTimeout();
                prependableSocket.setSoTimeout(newTimeout);
                bytesRead = prependableSocket.getInputStream().read(litmus, 0, litmus.length);
            } catch (SocketTimeoutException e) {
                                LOG.warn("Socket mode detection timed out after " + newTimeout + " ms, assuming PLAINTEXT");
            } finally {
                                try {
                    if (oldTimeout != -1) {
                        prependableSocket.setSoTimeout(oldTimeout);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to restore old socket timeout value of " + oldTimeout + " ms", e);
                }
            }
            if (bytesRead < 0) {                 bytesRead = 0;
            }

            if (bytesRead == litmus.length && SslHandler.isEncrypted(Unpooled.wrappedBuffer(litmus))) {
                try {
                    sslSocket = x509Util.createSSLSocket(prependableSocket, litmus);
                } catch (X509Exception e) {
                    throw new IOException("failed to create SSL context", e);
                }
                prependableSocket = null;
                mode = Mode.TLS;
                LOG.info("Accepted TLS connection from {} - {} - {}", sslSocket.getRemoteSocketAddress(), sslSocket.getSession().getProtocol(), sslSocket.getSession().getCipherSuite());
            } else if (allowInsecureConnection) {
                prependableSocket.prependToInputStream(litmus, 0, bytesRead);
                mode = Mode.PLAINTEXT;
                LOG.info("Accepted plaintext connection from {}", prependableSocket.getRemoteSocketAddress());
            } else {
                prependableSocket.close();
                mode = Mode.PLAINTEXT;
                throw new IOException("Blocked insecure connection attempt");
            }
        }

        private Socket getSocketAllowUnknownMode() {
            if (isSecureSocket()) {
                return sslSocket;
            } else {                 return prependableSocket;
            }
        }

        
        private Socket getSocket() throws IOException {
            if (!isModeKnown()) {
                detectMode();
            }
            if (mode == Mode.TLS) {
                return sslSocket;
            } else {
                return prependableSocket;
            }
        }

        
        public SSLSocket getSslSocket() throws IOException {
            if (!isModeKnown()) {
                detectMode();
            }
            if (!isSecureSocket()) {
                throw new SocketException("Socket mode is not TLS");
            }
            return sslSocket;
        }

        
        @Override
        public void connect(SocketAddress endpoint) throws IOException {
            getSocketAllowUnknownMode().connect(endpoint);
        }

        
        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            getSocketAllowUnknownMode().connect(endpoint, timeout);
        }

        
        @Override
        public void bind(SocketAddress bindpoint) throws IOException {
            getSocketAllowUnknownMode().bind(bindpoint);
        }

        
        @Override
        public InetAddress getInetAddress() {
            return getSocketAllowUnknownMode().getInetAddress();
        }

        
        @Override
        public InetAddress getLocalAddress() {
            return getSocketAllowUnknownMode().getLocalAddress();
        }

        
        @Override
        public int getPort() {
            return getSocketAllowUnknownMode().getPort();
        }

        
        @Override
        public int getLocalPort() {
            return getSocketAllowUnknownMode().getLocalPort();
        }

        
        @Override
        public SocketAddress getRemoteSocketAddress() {
            return getSocketAllowUnknownMode().getRemoteSocketAddress();
        }

        
        @Override
        public SocketAddress getLocalSocketAddress() {
            return getSocketAllowUnknownMode().getLocalSocketAddress();
        }

        
        @Override
        public SocketChannel getChannel() {
            return getSocketAllowUnknownMode().getChannel();
        }

        
        @Override
        public InputStream getInputStream() throws IOException {
            return new UnifiedInputStream(this);
        }

        
        @Override
        public OutputStream getOutputStream() throws IOException {
            return new UnifiedOutputStream(this);
        }

        
        @Override
        public void setTcpNoDelay(boolean on) throws SocketException {
            getSocketAllowUnknownMode().setTcpNoDelay(on);
        }

        
        @Override
        public boolean getTcpNoDelay() throws SocketException {
            return getSocketAllowUnknownMode().getTcpNoDelay();
        }

        
        @Override
        public void setSoLinger(boolean on, int linger) throws SocketException {
            getSocketAllowUnknownMode().setSoLinger(on, linger);
        }

        
        @Override
        public int getSoLinger() throws SocketException {
            return getSocketAllowUnknownMode().getSoLinger();
        }

        
        @Override
        public void sendUrgentData(int data) throws IOException {
            getSocket().sendUrgentData(data);
        }

        
        @Override
        public void setOOBInline(boolean on) throws SocketException {
            getSocketAllowUnknownMode().setOOBInline(on);
        }

        
        @Override
        public boolean getOOBInline() throws SocketException {
            return getSocketAllowUnknownMode().getOOBInline();
        }

        
        @Override
        public synchronized void setSoTimeout(int timeout) throws SocketException {
            getSocketAllowUnknownMode().setSoTimeout(timeout);
        }

        
        @Override
        public synchronized int getSoTimeout() throws SocketException {
            return getSocketAllowUnknownMode().getSoTimeout();
        }

        
        @Override
        public synchronized void setSendBufferSize(int size) throws SocketException {
            getSocketAllowUnknownMode().setSendBufferSize(size);
        }

        
        @Override
        public synchronized int getSendBufferSize() throws SocketException {
            return getSocketAllowUnknownMode().getSendBufferSize();
        }

        
        @Override
        public synchronized void setReceiveBufferSize(int size) throws SocketException {
            getSocketAllowUnknownMode().setReceiveBufferSize(size);
        }

        
        @Override
        public synchronized int getReceiveBufferSize() throws SocketException {
            return getSocketAllowUnknownMode().getReceiveBufferSize();
        }

        
        @Override
        public void setKeepAlive(boolean on) throws SocketException {
            getSocketAllowUnknownMode().setKeepAlive(on);
        }

        
        @Override
        public boolean getKeepAlive() throws SocketException {
            return getSocketAllowUnknownMode().getKeepAlive();
        }

        
        @Override
        public void setTrafficClass(int tc) throws SocketException {
            getSocketAllowUnknownMode().setTrafficClass(tc);
        }

        
        @Override
        public int getTrafficClass() throws SocketException {
            return getSocketAllowUnknownMode().getTrafficClass();
        }

        
        @Override
        public void setReuseAddress(boolean on) throws SocketException {
            getSocketAllowUnknownMode().setReuseAddress(on);
        }

        
        @Override
        public boolean getReuseAddress() throws SocketException {
            return getSocketAllowUnknownMode().getReuseAddress();
        }

        
        @Override
        public synchronized void close() throws IOException {
            getSocketAllowUnknownMode().close();
        }

        
        @Override
        public void shutdownInput() throws IOException {
            getSocketAllowUnknownMode().shutdownInput();
        }

        
        @Override
        public void shutdownOutput() throws IOException {
            getSocketAllowUnknownMode().shutdownOutput();
        }

        
        @Override
        public String toString() {
            return "UnifiedSocket[mode=" + mode.toString() + "socket=" + getSocketAllowUnknownMode().toString() + "]";
        }

        
        @Override
        public boolean isConnected() {
            return getSocketAllowUnknownMode().isConnected();
        }

        
        @Override
        public boolean isBound() {
            return getSocketAllowUnknownMode().isBound();
        }

        
        @Override
        public boolean isClosed() {
            return getSocketAllowUnknownMode().isClosed();
        }

        
        @Override
        public boolean isInputShutdown() {
            return getSocketAllowUnknownMode().isInputShutdown();
        }

        
        @Override
        public boolean isOutputShutdown() {
            return getSocketAllowUnknownMode().isOutputShutdown();
        }

        
        @Override
        public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
            getSocketAllowUnknownMode().setPerformancePreferences(connectionTime, latency, bandwidth);
        }
    }

    
    private static class UnifiedInputStream extends InputStream {
        private final UnifiedSocket unifiedSocket;
        private InputStream realInputStream;

        private UnifiedInputStream(UnifiedSocket unifiedSocket) {
            this.unifiedSocket = unifiedSocket;
            this.realInputStream = null;
        }

        @Override
        public int read() throws IOException {
            return getRealInputStream().read();
        }

        
        @Override
        public int read(byte[] b) throws IOException {
            return getRealInputStream().read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return getRealInputStream().read(b, off, len);
        }

        private InputStream getRealInputStream() throws IOException {
            if (realInputStream == null) {
                                realInputStream = unifiedSocket.getSocket().getInputStream();
            }
            return realInputStream;
        }

        @Override
        public long skip(long n) throws IOException {
            return getRealInputStream().skip(n);
        }

        @Override
        public int available() throws IOException {
            return getRealInputStream().available();
        }

        @Override
        public void close() throws IOException {
            getRealInputStream().close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            try {
                getRealInputStream().mark(readlimit);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public synchronized void reset() throws IOException {
            getRealInputStream().reset();
        }

        @Override
        public boolean markSupported() {
            try {
                return getRealInputStream().markSupported();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static class UnifiedOutputStream extends OutputStream {
        private final UnifiedSocket unifiedSocket;
        private OutputStream realOutputStream;

        private UnifiedOutputStream(UnifiedSocket unifiedSocket) {
            this.unifiedSocket = unifiedSocket;
            this.realOutputStream = null;
        }

        @Override
        public void write(int b) throws IOException {
            getRealOutputStream().write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            getRealOutputStream().write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            getRealOutputStream().write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            getRealOutputStream().flush();
        }

        @Override
        public void close() throws IOException {
            getRealOutputStream().close();
        }

        private OutputStream getRealOutputStream() throws IOException {
            if (realOutputStream == null) {
                                realOutputStream = unifiedSocket.getSocket().getOutputStream();
            }
            return realOutputStream;
        }

    }
}
