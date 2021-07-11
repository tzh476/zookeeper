package org.apache.zookeeper.common;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NettyUtils.class);

    private static final int DEFAULT_INET_ADDRESS_COUNT = 1;

    
    public static EventLoopGroup newNioOrEpollEventLoopGroup() {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup();
        } else {
            return new NioEventLoopGroup();
        }
    }

    
    public static EventLoopGroup newNioOrEpollEventLoopGroup(int nThreads) {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(nThreads);
        } else {
            return new NioEventLoopGroup(nThreads);
        }
    }

    
    public static Class<? extends SocketChannel> nioOrEpollSocketChannel() {
        if (Epoll.isAvailable()) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }

    
    public static Class<? extends ServerSocketChannel> nioOrEpollServerSocketChannel() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    
    public static int getClientReachableLocalInetAddressCount() {
        try {
            Set<InetAddress> validInetAddresses = new HashSet<>();
            Enumeration<NetworkInterface> allNetworkInterfaces = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface networkInterface : Collections.list(allNetworkInterfaces)) {
                for (InetAddress inetAddress : Collections.list(networkInterface.getInetAddresses())) {
                    if (inetAddress.isLinkLocalAddress()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring link-local InetAddress {}", inetAddress);
                        }
                        continue;
                    }
                    if (inetAddress.isMulticastAddress()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring multicast InetAddress {}", inetAddress);
                        }
                        continue;
                    }
                    if (inetAddress.isLoopbackAddress()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring loopback InetAddress {}", inetAddress);
                        }
                        continue;
                    }
                    validInetAddresses.add(inetAddress);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Detected {} local network addresses", validInetAddresses.size());
                LOG.debug("Resolved local addresses are: {}", Arrays.toString(validInetAddresses.toArray()));
            }
            return validInetAddresses.size() > 0 ? validInetAddresses.size() : DEFAULT_INET_ADDRESS_COUNT;
        } catch (SocketException ex) {
            LOG.warn("Failed to list all network interfaces, assuming 1", ex);
            return DEFAULT_INET_ADDRESS_COUNT;
        }
    }
}
