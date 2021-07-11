package org.apache.zookeeper.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

public class DeserializationPerfTest extends ZKTestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(DeserializationPerfTest.class);

    private static void deserializeTree(int depth, int width, int len)
            throws InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        BinaryInputArchive ia;
        int count;
        {
            DataTree tree = new DataTree();
            SerializationPerfTest.createNodes(tree, "/", depth, width, tree.getNode("/").stat.getCversion(), new byte[len]);
            count = tree.getNodeCount();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive oa = BinaryOutputArchive.getArchive(baos);
            tree.serialize(oa, "test");
            baos.flush();

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ia = BinaryInputArchive.getArchive(bais);
        }

        DataTree dserTree = new DataTree();

        System.gc();
        long start = System.nanoTime();
        dserTree.deserialize(ia, "test");
        long end = System.nanoTime();
        long durationms = (end - start) / 1000000L;
        long pernodeus = ((end - start) / 1000L) / count;

        Assert.assertEquals(count, dserTree.getNodeCount());

        LOG.info("Deserialized " + count + " nodes in " + durationms
                + " ms (" + pernodeus + "us/node), depth=" + depth + " width="
                + width + " datalen=" + len);
    }

    @Test
    public void testSingleDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(1, 0, 20);
    }

    @Test
    public void testWideDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(2, 10000, 20);
    }

    @Test
    public void testDeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(400, 1, 20);
    }

    @Test
    public void test10Wide5DeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(5, 10, 20);
    }

    @Test
    public void test15Wide5DeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(5, 15, 20);
    }

    @Test
    public void test25Wide4DeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(4, 25, 20);
    }

    @Test
    public void test40Wide4DeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(4, 40, 20);
    }

    @Test
    public void test300Wide3DeepDeserialize() throws
            InterruptedException, IOException, KeeperException.NodeExistsException, KeeperException.NoNodeException {
        deserializeTree(3, 300, 20);
    }

}
