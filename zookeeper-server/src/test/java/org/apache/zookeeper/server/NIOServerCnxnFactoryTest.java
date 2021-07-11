package org.apache.zookeeper.server;

import org.apache.zookeeper.PortAssignment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;

public class NIOServerCnxnFactoryTest {
    private InetSocketAddress listenAddress;
    private NIOServerCnxnFactory factory;

    @Before
    public void setUp() throws IOException {
        listenAddress = new InetSocketAddress(PortAssignment.unique());
        factory = new NIOServerCnxnFactory();
        factory.configure(listenAddress, 100);
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.shutdown();
        }
    }

    @Test(expected = SocketException.class)
    public void testStartupWithoutStart_SocketAlreadyBound() throws IOException {
        ServerSocket ss = new ServerSocket(listenAddress.getPort());
    }

    @Test(expected = SocketException.class)
    public void testStartupWithStart_SocketAlreadyBound() throws IOException {
        factory.start();
        ServerSocket ss = new ServerSocket(listenAddress.getPort());
    }

    @Test
    public void testShutdownWithoutStart_SocketReleased() throws IOException {
        factory.shutdown();
        factory = null;

        ServerSocket ss = new ServerSocket(listenAddress.getPort());
        ss.close();
    }
}
