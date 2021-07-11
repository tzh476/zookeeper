package org.apache.zookeeper.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.proto.ConnectRequest;
import org.junit.Assert;
import org.junit.Test;

public class MaxCnxnsTest extends ClientBase {
    final private static int numCnxns = 30;
    AtomicInteger numConnected = new AtomicInteger(0);
    String host;
    int port;

    @Override
    public void setUp() throws Exception {
        maxCnxns = numCnxns;
        super.setUp();
    }

    class CnxnThread extends Thread {

        public CnxnThread(int i) {
            super("CnxnThread-"+i);
        }

        public void run() {
            SocketChannel sChannel = null;
            try {
                
                sChannel = SocketChannel.open();
                sChannel.connect(new InetSocketAddress(host,port));
                                ConnectRequest conReq = new ConnectRequest(0, 0,
                        10000, 0, "password".getBytes());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len");
                conReq.serialize(boa, "connect");
                baos.close();
                ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
                bb.putInt(bb.capacity() - 4);
                bb.rewind();

                

                int eof = sChannel.write(bb);
                                                sChannel.socket().setSoTimeout(10000);
                if (!sChannel.socket().isClosed()){
                    eof = sChannel.socket().getInputStream().read();
                    if (eof != -1) {
                        numConnected.incrementAndGet();
                    }
                }
            }
            catch (IOException io) {
                            } finally {
                if (sChannel != null) {
                    try {
                        sChannel.close();
                    } catch (Exception e) {
                                            }
                }
            }
        }
    }

    
    @Test
    public void testMaxCnxns() throws IOException, InterruptedException{
        String split[] = hostPort.split(":");
        host = split[0];
        port = Integer.parseInt(split[1]);
        int numThreads = numCnxns + 5;
        CnxnThread[] threads = new CnxnThread[numThreads];

        for (int i=0;i<numCnxns;++i) {
          threads[i] = new CnxnThread(i);
        }

        for (int i=0;i<numCnxns;++i) {
            threads[i].start();
        }

        for (int i=0;i<numCnxns;++i) {
            threads[i].join();
        }
        Assert.assertSame(numCnxns,numConnected.get());
    }
}
