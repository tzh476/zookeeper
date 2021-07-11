package org.apache.zookeeper;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.Assert;
import org.junit.Test;

public class ClientRequestTimeoutTest extends QuorumPeerTestBase {
    private static final int SERVER_COUNT = 3;
    private boolean dropPacket = false;
    private int dropPacketType = ZooDefs.OpCode.create;

    @Test(timeout = 120000)
    public void testClientRequestTimeout() throws Exception {
        int requestTimeOut = 15000;
        System.setProperty("zookeeper.request.timeout",
                Integer.toString(requestTimeOut));
        final int clientPorts[] = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        String server;

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=127.0.0.1:" + PortAssignment.unique()
                    + ":" + PortAssignment.unique() + ":participant;127.0.0.1:"
                    + clientPorts[i];
            sb.append(server + "\n");
        }
        String currentQuorumCfgSection = sb.toString();
        MainThread mt[] = new MainThread[SERVER_COUNT];

        for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    false);
            mt[i].start();
        }

                for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT));
        }

        CountdownWatcher watch1 = new CountdownWatcher();
        CustomZooKeeper zk = new CustomZooKeeper(getCxnString(clientPorts),
                ClientBase.CONNECTION_TIMEOUT, watch1);
        watch1.waitForConnected(ClientBase.CONNECTION_TIMEOUT);

        String data = "originalData";
                zk.create("/clientHang1", data.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);

                dropPacket = true;
        dropPacketType = ZooDefs.OpCode.create;

                try {
            zk.create("/clientHang2", data.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            fail("KeeperException is expected.");
        } catch (KeeperException exception) {
            assertEquals(KeeperException.Code.REQUESTTIMEOUT.intValue(),
                    exception.code().intValue());
        }

                zk.close();
        for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i].shutdown();
        }
    }

    
    private String getCxnString(int[] clientPorts) {
        StringBuffer hostPortBuffer = new StringBuffer();
        for (int i = 0; i < clientPorts.length; i++) {
            hostPortBuffer.append("127.0.0.1:");
            hostPortBuffer.append(clientPorts[i]);
            if (i != (clientPorts.length - 1)) {
                hostPortBuffer.append(',');
            }
        }
        return hostPortBuffer.toString();
    }

    class CustomClientCnxn extends ClientCnxn {

        public CustomClientCnxn(String chrootPath, HostProvider hostProvider,
                int sessionTimeout, ZooKeeper zooKeeper,
                ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
                boolean canBeReadOnly) throws IOException {
            super(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher,
                    clientCnxnSocket, canBeReadOnly);
        }

        @Override
        public void finishPacket(Packet p) {
            if (dropPacket && p.requestHeader.getType() == dropPacketType) {
                                                return;
            }
            super.finishPacket(p);
        }
    }

    class CustomZooKeeper extends ZooKeeper {
        public CustomZooKeeper(String connectString, int sessionTimeout,
                Watcher watcher) throws IOException {
            super(connectString, sessionTimeout, watcher);
        }

        @Override
        protected ClientCnxn createConnection(String chrootPath,
                HostProvider hostProvider, int sessionTimeout,
                ZooKeeper zooKeeper, ClientWatchManager watcher,
                ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly)
                        throws IOException {
            return new CustomClientCnxn(chrootPath, hostProvider,
                    sessionTimeout, zooKeeper, watcher, clientCnxnSocket,
                    canBeReadOnly);
        }
    }
}
