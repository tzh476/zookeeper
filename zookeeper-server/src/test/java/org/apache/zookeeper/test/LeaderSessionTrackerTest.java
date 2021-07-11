package org.apache.zookeeper.test;


import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeaderSessionTrackerTest extends ZKTestCase {

    protected static final Logger LOG = LoggerFactory
            .getLogger(LeaderSessionTrackerTest.class);

    QuorumUtil qu;

    @Before
    public void setUp() throws Exception {
        qu = new QuorumUtil(1);
    }

    @After
    public void tearDown() throws Exception {
        qu.shutdownAll();
    }

    @Test
    public void testExpiredSessionWithLocalSession() throws Exception {
        testCreateEphemeral(true);
    }

    @Test
    public void testExpiredSessionWithoutLocalSession() throws Exception {
        testCreateEphemeral(false);
    }

    
    public void testCreateEphemeral(boolean localSessionEnabled) throws Exception {
        if (localSessionEnabled) {
            qu.enableLocalSession(true);
        }
        qu.startAll();
        QuorumPeer leader = qu.getLeaderQuorumPeer();

        ZooKeeper zk = ClientBase.createZKClient(qu.getConnectString(leader));

        CreateRequest createRequest = new CreateRequest("/impossible",
                new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL.toFlag());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        createRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                long sid = qu.getFollowerQuorumPeers().get(0).getActiveServer()
                .getServerId();
        long fakeSessionId = (sid << 56) + 1;

        LOG.info("Fake session Id: " + Long.toHexString(fakeSessionId));

        Request request = new Request(null, fakeSessionId, 0, OpCode.create,
                bb, new ArrayList<Id>());

                leader.getActiveServer().submitRequest(request);

                zk.create("/ok", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        Stat stat = zk.exists("/impossible", null);
        Assert.assertEquals("Node from fake session get created", null, stat);

    }

    
    @Test
    public void testCreatePersistent() throws Exception {
        qu.enableLocalSession(true);
        qu.startAll();

        QuorumPeer leader = qu.getLeaderQuorumPeer();

        ZooKeeper zk = ClientBase.createZKClient(qu.getConnectString(leader));

        CreateRequest createRequest = new CreateRequest("/success",
                new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT.toFlag());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        createRequest.serialize(boa, "request");
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                long sid = qu.getFollowerQuorumPeers().get(0).getActiveServer()
                .getServerId();
        long locallSession = (sid << 56) + 1;

        LOG.info("Local session Id: " + Long.toHexString(locallSession));

        Request request = new Request(null, locallSession, 0, OpCode.create,
                bb, new ArrayList<Id>());

                leader.getActiveServer().submitRequest(request);

                zk.create("/ok", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        Stat stat = zk.exists("/success", null);
        Assert.assertTrue("Request from local sesson failed", stat != null);

    }
}
