package org.apache.zookeeper.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerCnxn.class);
    private final Channel channel;
    private CompositeByteBuf queuedBuffer;
    private final AtomicBoolean throttled = new AtomicBoolean(false);
    private ByteBuffer bb;
    private final ByteBuffer bbLen = ByteBuffer.allocate(4);
    private long sessionId;
    private int sessionTimeout;
    private AtomicLong outstandingCount = new AtomicLong();
    private Certificate[] clientChain;
    private volatile boolean closingChannel;

    
    private volatile ZooKeeperServer zkServer;

    private final NettyServerCnxnFactory factory;
    private boolean initialized;

    NettyServerCnxn(Channel channel, ZooKeeperServer zks, NettyServerCnxnFactory factory) {
        this.channel = channel;
        this.closingChannel = false;
        this.zkServer = zks;
        this.factory = factory;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
    }

    @Override
    public void close() {
        closingChannel = true;

        if (LOG.isDebugEnabled()) {
            LOG.debug("close called for sessionid:0x{}",
                    Long.toHexString(sessionId));
        }

                                factory.unregisterConnection(this);

                if (!factory.cnxns.remove(this)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cnxns size:{}", factory.cnxns.size());
            }
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("close in progress for sessionid:0x{}",
                    Long.toHexString(sessionId));
        }

        factory.removeCnxnFromIpMap(
                this,
                ((InetSocketAddress)channel.remoteAddress()).getAddress());

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (channel.isOpen()) {
                                                channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    future.channel().close().addListener(f -> releaseQueuedBuffer());
                }
            });
        } else {
            channel.eventLoop().execute(this::releaseQueuedBuffer);
        }
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

                WatcherEvent e = event.getWrapper();

        try {
            sendResponse(h, e, "notification");
        } catch (IOException e1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Problem sending to " + getRemoteSocketAddress(), e1);
            }
            close();
        }
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag)
            throws IOException {
        if (closingChannel || !channel.isOpen()) {
            return;
        }
        super.sendResponse(h, r, tag);
        if (h.getXid() > 0) {
                        if (!zkServer.shouldThrottle(outstandingCount.decrementAndGet())) {
                enableRecv();
            }
        }
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

        private final GenericFutureListener<Future<Void>> onSendBufferDoneListener = f -> {
        if (f.isSuccess()) {
            packetSent();
        }
    };

    @Override
    public void sendBuffer(ByteBuffer sendBuffer) {
        if (sendBuffer == ServerCnxnFactory.closeConn) {
            close();
            return;
        }
        channel.writeAndFlush(Unpooled.wrappedBuffer(sendBuffer)).addListener(onSendBufferDoneListener);
    }

    
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBuffer(ByteBuffer.wrap(sb.toString().getBytes()));
                                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            checkFlush(true);
            sb = null;         }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }

    
    private boolean checkFourLetterWord(final Channel channel, ByteBuf message, final int len) {
                        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);

                                channel.config().setAutoRead(false);
        packetReceived();

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

                if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd +
                    " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing {} command from {}", cmd, channel.remoteAddress());

       if (len == FourLetterCommands.setTraceMaskCmd) {
            ByteBuffer mask = ByteBuffer.allocate(8);
            message.readBytes(mask);
            mask.flip();
            long traceMask = mask.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer,factory);
        }
    }

    
    private void checkIsInEventLoop(String callerMethodName) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException(
                    callerMethodName + "() called from non-EventLoop thread");
        }
    }

    
    private void appendToQueuedBuffer(ByteBuf buf) {
        checkIsInEventLoop("appendToQueuedBuffer");
        if (queuedBuffer.numComponents() == queuedBuffer.maxNumComponents()) {
                        queuedBuffer.consolidate();
        }
        queuedBuffer.addComponent(true, buf);
    }

    
    void processMessage(ByteBuf buf) {
        checkIsInEventLoop("processMessage");
        if (LOG.isDebugEnabled()) {
            LOG.debug("0x{} queuedBuffer: {}",
                    Long.toHexString(sessionId),
                    queuedBuffer);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("0x{} buf {}",
                    Long.toHexString(sessionId),
                    ByteBufUtil.hexDump(buf));
        }

        if (throttled.get()) {
            LOG.debug("Received message while throttled");
                        if (queuedBuffer == null) {
                LOG.debug("allocating queue");
                queuedBuffer = channel.alloc().compositeBuffer();
            }
            appendToQueuedBuffer(buf.retainedDuplicate());
            if (LOG.isTraceEnabled()) {
                LOG.trace("0x{} queuedBuffer {}",
                        Long.toHexString(sessionId),
                        ByteBufUtil.hexDump(queuedBuffer));
            }
        } else {
            LOG.debug("not throttled");
            if (queuedBuffer != null) {
                appendToQueuedBuffer(buf.retainedDuplicate());
                processQueuedBuffer();
            } else {
                receiveMessage(buf);
                                                if (!closingChannel && buf.isReadable()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Before copy {}", buf);
                    }
                    if (queuedBuffer == null) {
                        queuedBuffer = channel.alloc().compositeBuffer();
                    }
                    appendToQueuedBuffer(buf.retainedSlice(buf.readerIndex(), buf.readableBytes()));
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Copy is {}", queuedBuffer);
                        LOG.trace("0x{} queuedBuffer {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(queuedBuffer));
                    }
                }
            }
        }
    }

    
    void processQueuedBuffer() {
        checkIsInEventLoop("processQueuedBuffer");
        if (queuedBuffer != null) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("processing queue 0x{} queuedBuffer {}",
                        Long.toHexString(sessionId),
                        ByteBufUtil.hexDump(queuedBuffer));
            }
            receiveMessage(queuedBuffer);
            if (closingChannel) {
                                LOG.debug("Processed queue - channel closed, dropping remaining bytes");
            } else if (!queuedBuffer.isReadable()) {
                LOG.debug("Processed queue - no bytes remaining");
                releaseQueuedBuffer();
            } else {
                LOG.debug("Processed queue - bytes remaining");
                                                queuedBuffer.discardReadComponents();
            }
        } else {
            LOG.debug("queue empty");
        }
    }

    
    private void releaseQueuedBuffer() {
        checkIsInEventLoop("releaseQueuedBuffer");
        if (queuedBuffer != null) {
            queuedBuffer.release();
            queuedBuffer = null;
        }
    }

    
    private void receiveMessage(ByteBuf message) {
        checkIsInEventLoop("receiveMessage");
        try {
            while(message.isReadable() && !throttled.get()) {
                if (bb != null) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("message readable {} bb len {} {}",
                                message.readableBytes(),
                                bb.remaining(),
                                bb);
                        ByteBuffer dat = bb.duplicate();
                        dat.flip();
                        LOG.trace("0x{} bb {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }

                    if (bb.remaining() > message.readableBytes()) {
                        int newLimit = bb.position() + message.readableBytes();
                        bb.limit(newLimit);
                    }
                    message.readBytes(bb);
                    bb.limit(bb.capacity());

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("after readBytes message readable {} bb len {} {}",
                                message.readableBytes(),
                                bb.remaining(),
                                bb);
                        ByteBuffer dat = bb.duplicate();
                        dat.flip();
                        LOG.trace("after readbytes 0x{} bb {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }
                    if (bb.remaining() == 0) {
                        packetReceived();
                        bb.flip();

                        ZooKeeperServer zks = this.zkServer;
                        if (zks == null || !zks.isRunning()) {
                            throw new IOException("ZK down");
                        }
                        if (initialized) {
                                                                                    zks.processPacket(this, bb);

                            if (zks.shouldThrottle(outstandingCount.incrementAndGet())) {
                                disableRecvNoWait();
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("got conn req request from {}",
                                        getRemoteSocketAddress());
                            }
                            zks.processConnectRequest(this, bb);
                            initialized = true;
                        }
                        bb = null;
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("message readable {} bblenrem {}",
                                message.readableBytes(),
                                bbLen.remaining());
                        ByteBuffer dat = bbLen.duplicate();
                        dat.flip();
                        LOG.trace("0x{} bbLen {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }

                    if (message.readableBytes() < bbLen.remaining()) {
                        bbLen.limit(bbLen.position() + message.readableBytes());
                    }
                    message.readBytes(bbLen);
                    bbLen.limit(bbLen.capacity());
                    if (bbLen.remaining() == 0) {
                        bbLen.flip();

                        if (LOG.isTraceEnabled()) {
                            LOG.trace("0x{} bbLen {}",
                                    Long.toHexString(sessionId),
                                    ByteBufUtil.hexDump(Unpooled.wrappedBuffer(bbLen)));
                        }
                        int len = bbLen.getInt();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("0x{} bbLen len is {}",
                                    Long.toHexString(sessionId),
                                    len);
                        }

                        bbLen.clear();
                        if (!initialized) {
                            if (checkFourLetterWord(channel, message, len)) {
                                return;
                            }
                        }
                        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
                            throw new IOException("Len error " + len);
                        }
                        bb = ByteBuffer.allocate(len);
                    }
                }
            }
        } catch(IOException e) {
            LOG.warn("Closing connection to " + getRemoteSocketAddress(), e);
            close();
        }
    }

    
    enum AutoReadEvent {
        DISABLE,
        ENABLE
    }

    
    @Override
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Throttling - disabling recv {}", this);
            }
            channel.pipeline().fireUserEventTriggered(AutoReadEvent.DISABLE);
        }
    }

    private void disableRecvNoWait() {
        disableRecv();
    }

    @Override
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending unthrottle event {}", this);
            }
            channel.pipeline().fireUserEventTriggered(AutoReadEvent.ENABLE);
        }
    }

    @Override
    public long getOutstandingRequests() {
        return outstandingCount.longValue();
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public int getInterestOps() {
                        if (channel == null || !channel.isOpen()) {
            return 0;
        }
        int interestOps = 0;
        if (!throttled.get()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (!channel.isWritable()) {
                                    interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress)channel.remoteAddress();
    }

    
    @Override
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return factory.secure;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        if (clientChain == null)
        {
            return null;
        }
        return Arrays.copyOf(clientChain, clientChain.length);
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        if (chain == null)
        {
            clientChain = null;
        } else {
            clientChain = Arrays.copyOf(chain, chain.length);
        }
    }

        Channel getChannel() {
        return channel;
    }
}
