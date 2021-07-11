package org.apache.zookeeper.server;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;

public class Emulate353TTLTest extends ClientBase {
    private TestableZooKeeper zk;

    @Override
    public void setUp() throws Exception {
        System.setProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY, "true");
        System.setProperty(EphemeralType.TTL_3_5_3_EMULATION_PROPERTY, "true");
        super.setUp();
        zk = createClient();
    }

    @Override
    public void tearDown() throws Exception {
        System.clearProperty(EphemeralType.EXTENDED_TYPES_ENABLED_PROPERTY);
        System.clearProperty(EphemeralType.TTL_3_5_3_EMULATION_PROPERTY);
        super.tearDown();
        zk.close();
    }

    @Test
    public void testCreate()
            throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        zk.create("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_WITH_TTL, stat, 100);
        Assert.assertEquals(0, stat.getEphemeralOwner());

        final AtomicLong fakeElapsed = new AtomicLong(0);
        ContainerManager containerManager = newContainerManager(fakeElapsed);
        containerManager.checkContainers();
        Assert.assertNotNull("Ttl node should not have been deleted yet", zk.exists("/foo", false));

        fakeElapsed.set(1000);
        containerManager.checkContainers();
        Assert.assertNull("Ttl node should have been deleted", zk.exists("/foo", false));
    }

    @Test
    public void test353TTL()
            throws KeeperException, InterruptedException {
        DataTree dataTree = serverFactory.zkServer.getZKDatabase().dataTree;
        long ephemeralOwner = EphemeralTypeEmulate353.ttlToEphemeralOwner(100);
        dataTree.createNode("/foo", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, ephemeralOwner,
                dataTree.getNode("/").stat.getCversion()+1, 1, 1);

        final AtomicLong fakeElapsed = new AtomicLong(0);
        ContainerManager containerManager = newContainerManager(fakeElapsed);
        containerManager.checkContainers();
        Assert.assertNotNull("Ttl node should not have been deleted yet", zk.exists("/foo", false));

        fakeElapsed.set(1000);
        containerManager.checkContainers();
        Assert.assertNull("Ttl node should have been deleted", zk.exists("/foo", false));
    }

    @Test
    public void testEphemeralOwner_emulationTTL() {
        Assert.assertThat(EphemeralType.get(-1), equalTo(EphemeralType.TTL));
    }

    @Test
    public void testEphemeralOwner_emulationContainer() {
        Assert.assertThat(EphemeralType.get(EphemeralType.CONTAINER_EPHEMERAL_OWNER), equalTo(EphemeralType.CONTAINER));
    }

    private ContainerManager newContainerManager(final AtomicLong fakeElapsed) {
        return new ContainerManager(serverFactory.getZooKeeperServer()
                .getZKDatabase(), serverFactory.getZooKeeperServer().firstProcessor, 1, 100) {
            @Override
            protected long getElapsed(DataNode node) {
                return fakeElapsed.get();
            }
        };
    }
}
