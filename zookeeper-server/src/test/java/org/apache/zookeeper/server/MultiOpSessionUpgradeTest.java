package org.apache.zookeeper.server;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumZooKeeperServer;
import org.apache.zookeeper.server.quorum.UpgradeableSessionTracker;
import org.apache.zookeeper.test.QuorumBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiOpSessionUpgradeTest extends QuorumBase {
    protected static final Logger LOG = LoggerFactory.getLogger(MultiOpSessionUpgradeTest.class);

    @Override
    public void setUp() throws Exception {
        localSessionsEnabled = true;
        localSessionsUpgradingEnabled = true;
        super.setUp();
    }

    @Test
    public void ephemeralCreateMultiOpTest() throws KeeperException, InterruptedException, IOException {
        final ZooKeeper zk = createClient();

        String data = "test";
        String path = "/ephemeralcreatemultiop";
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        QuorumZooKeeperServer server = getConnectedServer(zk.getSessionId());
        Assert.assertNotNull("unable to find server interlocutor", server);
        UpgradeableSessionTracker sessionTracker = (UpgradeableSessionTracker)server.getSessionTracker();
        Assert.assertFalse("session already global", sessionTracker.isGlobalSession(zk.getSessionId()));

        List<OpResult> multi = null;
        try {
            multi = zk.multi(Arrays.asList(
                    Op.setData(path, data.getBytes(), 0),
                    Op.create(path + "/e", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                    Op.create(path + "/p", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                    Op.create(path + "/q", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
            ));
        } catch (KeeperException.SessionExpiredException e) {
                        Assert.fail("received session expired for a session promotion in a multi-op");
        }

        Assert.assertNotNull(multi);
        Assert.assertEquals(4, multi.size());
        Assert.assertEquals(data, new String(zk.getData(path + "/e", false, null)));
        Assert.assertEquals(data, new String(zk.getData(path + "/p", false, null)));
        Assert.assertEquals(data, new String(zk.getData(path + "/q", false, null)));
        Assert.assertTrue("session not promoted", sessionTracker.isGlobalSession(zk.getSessionId()));
    }

    @Test
    public void directCheckUpgradeSessionTest() throws IOException, InterruptedException, KeeperException {
        final ZooKeeper zk = createClient();

        String path = "/directcheckupgradesession";
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        QuorumZooKeeperServer server = getConnectedServer(zk.getSessionId());
        Assert.assertNotNull("unable to find server interlocutor", server);

        Request readRequest = makeGetDataRequest(path, zk.getSessionId());
        Request createRequest = makeCreateRequest(path + "/e", zk.getSessionId());
        Assert.assertNull("tried to upgrade on a read", server.checkUpgradeSession(readRequest));
        Assert.assertNotNull("failed to upgrade on a create", server.checkUpgradeSession(createRequest));
        Assert.assertNull("tried to upgrade after successful promotion",
                server.checkUpgradeSession(createRequest));
    }

    private Request makeGetDataRequest(String path, long sessionId) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(boas);
        GetDataRequest getDataRequest = new GetDataRequest(path, false);
        getDataRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(boas.toByteArray());
        return new Request(null, sessionId, 1, ZooDefs.OpCode.getData, bb, new ArrayList<Id>());
    }

    private Request makeCreateRequest(String path, long sessionId) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(boas);
        CreateRequest createRequest = new CreateRequest(path,
                "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL.toFlag());
        createRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(boas.toByteArray());
        return new Request(null, sessionId, 1, ZooDefs.OpCode.create2, bb, new ArrayList<Id>());
    }

    private QuorumZooKeeperServer getConnectedServer(long sessionId) {
        for (QuorumPeer peer : getPeerList()) {
            if (peer.getActiveServer().getSessionTracker().isTrackingSession(sessionId)) {
                return (QuorumZooKeeperServer)peer.getActiveServer();
            }
        }
        return null;
    }
}
