package org.apache.zookeeper.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.zookeeper.common.ClientX509Util;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.common.X509Exception.SSLContextException;
import org.apache.zookeeper.common.X509Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
public class FourLetterWordMain {
        private static final int DEFAULT_SOCKET_TIMEOUT = 5000;
    protected static final Logger LOG = LoggerFactory.getLogger(FourLetterWordMain.class);
    
    public static String send4LetterWord(String host, int port, String cmd)
            throws IOException, SSLContextException {
        return send4LetterWord(host, port, cmd, false, DEFAULT_SOCKET_TIMEOUT);
    }

    
    public static String send4LetterWord(String host, int port, String cmd, boolean secure)
            throws IOException, SSLContextException {
        return send4LetterWord(host, port, cmd, secure, DEFAULT_SOCKET_TIMEOUT);
    }

    
    public static String send4LetterWord(String host, int port, String cmd, boolean secure, int timeout)
            throws IOException, SSLContextException {
        LOG.info("connecting to {} {}", host, port);
        Socket sock;
        InetSocketAddress hostaddress= host != null ? new InetSocketAddress(host, port) :
            new InetSocketAddress(InetAddress.getByName(null), port);
        if (secure) {
            LOG.info("using secure socket");
            try (X509Util x509Util = new ClientX509Util()) {
                SSLContext sslContext = x509Util.getDefaultSSLContext();
                SSLSocketFactory socketFactory = sslContext.getSocketFactory();
                SSLSocket sslSock = (SSLSocket) socketFactory.createSocket();
                sslSock.connect(hostaddress, timeout);
                sslSock.startHandshake();
                sock = sslSock;
            }
        } else {
            sock = new Socket();
            sock.connect(hostaddress, timeout);
        }
        sock.setSoTimeout(timeout);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();

                        if (!secure) {
                                sock.shutdownOutput();
            }

            reader =
                    new BufferedReader(
                            new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        } catch (SocketTimeoutException e) {
            throw new IOException("Exception while executing four letter word: " + cmd, e);
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }
    
    public static void main(String[] args)
            throws IOException, SSLContextException
    {
        if (args.length == 3) {
            System.out.println(send4LetterWord(args[0], Integer.parseInt(args[1]), args[2]));
        } else if (args.length == 4) {
            System.out.println(send4LetterWord(args[0], Integer.parseInt(args[1]), args[2], Boolean.parseBoolean(args[3])));
        } else {
            System.out.println("Usage: FourLetterWordMain <host> <port> <cmd> <secure(optional)>");
        }
    }
}
