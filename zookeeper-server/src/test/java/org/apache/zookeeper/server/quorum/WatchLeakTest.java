package org.apache.zookeeper.server.quorum;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Random;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.MockPacket;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.server.MockNIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.MockSelectorThread;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.ZKParameterized;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(ZKParameterized.RunnerFactory.class)
public class WatchLeakTest {

    protected static final Logger LOG = LoggerFactory
            .getLogger(WatchLeakTest.class);

    final long SESSION_ID = 0xBABEL;

    private final boolean sessionTimedout;

    @Before
    public void setUp() {
        System.setProperty("zookeeper.admin.enableServer", "false");
    }

    public WatchLeakTest(boolean sessionTimedout) {
        this.sessionTimedout = sessionTimedout;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { false }, { true },
        });
    }

    

    @Test
    public void testWatchesLeak() throws Exception {

        NIOServerCnxnFactory serverCnxnFactory = mock(NIOServerCnxnFactory.class);
        final SelectionKey sk = new FakeSK();
        MockSelectorThread selectorThread = mock(MockSelectorThread.class);
        when(selectorThread.addInterestOpsUpdateRequest(any(SelectionKey.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                SelectionKey sk = (SelectionKey)invocation.getArguments()[0];
                NIOServerCnxn nioSrvCnx = (NIOServerCnxn)sk.attachment();
                sk.interestOps(nioSrvCnx.getInterestOps());
                return true;
            }
        });

        ZKDatabase database = new ZKDatabase(null);
        database.setlastProcessedZxid(2L);
        QuorumPeer quorumPeer = mock(QuorumPeer.class);
        FileTxnSnapLog logfactory = mock(FileTxnSnapLog.class);
                when(logfactory.getDataDir()).thenReturn(new File(""));
        when(logfactory.getSnapDir()).thenReturn(new File(""));
        FollowerZooKeeperServer fzks = null;

        try {
                        fzks = new FollowerZooKeeperServer(logfactory, quorumPeer, database);
            fzks.startup();
            fzks.setServerCnxnFactory(serverCnxnFactory);
            quorumPeer.follower = new MyFollower(quorumPeer, fzks);
            LOG.info("Follower created");
                        final SocketChannel socketChannel = createClientSocketChannel();
                        final MockNIOServerCnxn nioCnxn = new MockNIOServerCnxn(fzks,
                    socketChannel, sk, serverCnxnFactory, selectorThread);
            sk.attach(nioCnxn);
                        nioCnxn.doIO(sk);
            LOG.info("Client connection sent");
                        QuorumPacket qp = createValidateSessionPacketResponse(!sessionTimedout);
            quorumPeer.follower.processPacket(qp);
            LOG.info("Session validation sent");
                                    nioCnxn.doIO(sk);
                        Thread.sleep(1000L);
            LOG.info("Watches processed");
                        int watchCount = database.getDataTree().getWatchCount();
            if (sessionTimedout) {
                                LOG.info("session is not valid, watches = {}", watchCount);
                assertEquals("Session is not valid so there should be no watches", 0, watchCount);
            } else {
                                LOG.info("session is valid, watches = {}", watchCount);
                assertEquals("Session is valid so the watch should be there", 1, watchCount);
            }
        } finally {
            if (fzks != null) {
                fzks.shutdown();
            }
        }
    }

    
    public static class MyFollower extends Follower {
        
        MyFollower(QuorumPeer self, FollowerZooKeeperServer zk) {
            super(self, zk);
            leaderOs = mock(OutputArchive.class);
            leaderIs = mock(InputArchive.class);
            bufferedOutput = mock(BufferedOutputStream.class);
        }
    }

    
    private static class FakeSK extends SelectionKey {

        @Override
        public SelectableChannel channel() {
            return null;
        }

        @Override
        public Selector selector() {
            return mock(Selector.class);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public void cancel() {
        }

        @Override
        public int interestOps() {
            return ops;
        }

        private int ops = OP_WRITE + OP_READ;

        @Override
        public SelectionKey interestOps(int ops) {
            this.ops = ops;
            return this;
        }

        @Override
        public int readyOps() {
            boolean reading = (ops & OP_READ) != 0;
            boolean writing = (ops & OP_WRITE) != 0;
            if (reading && writing) {
                LOG.info("Channel is ready for reading and writing");
            } else if (reading) {
                LOG.info("Channel is ready for reading only");
            } else if (writing) {
                LOG.info("Channel is ready for writing only");
            }
            return ops;
        }

    }

    
    private ByteBuffer createWatchesMessage() {
        List<String> dataWatches = new ArrayList<String>(1);
        dataWatches.add("/");
        List<String> existWatches = Collections.emptyList();
        List<String> childWatches = Collections.emptyList();
        SetWatches sw = new SetWatches(1L, dataWatches, existWatches,
                childWatches);
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setWatches);
        h.setXid(-8);
        MockPacket p = new MockPacket(h, new ReplyHeader(), sw, null, null);
        return p.createAndReturnBB();
    }

    
    static final private long superSecret = 0XB3415C00L;

    
    private ByteBuffer createConnRequest() {
        Random r = new Random(SESSION_ID ^ superSecret);
        byte p[] = new byte[16];
        r.nextBytes(p);
        ConnectRequest conReq = new ConnectRequest(0, 1L, 30000, SESSION_ID, p);
        MockPacket packet = new MockPacket(null, null, conReq, null, null, false);
        return packet.createAndReturnBB();
    }

    
    private SocketChannel createClientSocketChannel() throws IOException {

        SocketChannel socketChannel = mock(SocketChannel.class);
        Socket socket = mock(Socket.class);
        InetSocketAddress socketAddress = new InetSocketAddress(1234);
        when(socket.getRemoteSocketAddress()).thenReturn(socketAddress);
        when(socketChannel.socket()).thenReturn(socket);

                final ByteBuffer connRequest = createConnRequest();
        final ByteBuffer watchesMessage = createWatchesMessage();
        final ByteBuffer request = ByteBuffer.allocate(connRequest.limit()
                + watchesMessage.limit());
        request.put(connRequest);
        request.put(watchesMessage);

        Answer<Integer> answer = new Answer<Integer>() {
            int i = 0;

            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                ByteBuffer bb = (ByteBuffer) args[0];
                for (int k = 0; k < bb.limit(); k++) {
                    bb.put(request.get(i));
                    i = i + 1;
                }
                return bb.limit();
            }
        };
        when(socketChannel.read(any(ByteBuffer.class))).thenAnswer(answer);
        return socketChannel;
    }

    
    private QuorumPacket createValidateSessionPacketResponse(boolean valid) throws Exception {
        QuorumPacket qp = createValidateSessionPacket();
        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
        DataInputStream dis = new DataInputStream(bis);
        long id = dis.readLong();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(id);
                dos.writeBoolean(valid);
        qp.setData(bos.toByteArray());
        return qp;
    }

    
    private QuorumPacket createValidateSessionPacket() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(SESSION_ID);
        dos.writeInt(3000);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1,
                baos.toByteArray(), null);
        return qp;
    }

}
