package org.apache.zookeeper.test;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class CreateTest extends ClientBase {
  private ZooKeeper zk;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    zk = createClient();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    zk.close();
  }

  @Test
  public void testCreate()
      throws IOException, KeeperException, InterruptedException {
    createNoStatVerifyResult("/foo");
    createNoStatVerifyResult("/foo/child");
  }

  @Test
  public void testCreateWithStat()
      throws IOException, KeeperException, InterruptedException {
    String name = "/foo";
    Stat stat = createWithStatVerifyResult("/foo");
    Stat childStat = createWithStatVerifyResult("/foo/child");
        Assert.assertFalse(stat.equals(childStat));
  }

  @Test
  public void testCreateWithNullStat()
      throws IOException, KeeperException, InterruptedException {
    String name = "/foo";
    Assert.assertNull(zk.exists(name, false));

    Stat stat = null;
            String path = zk.create(name, name.getBytes(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
    Assert.assertNull(stat);
    Assert.assertNotNull(zk.exists(name, false));
  }

  private void createNoStatVerifyResult(String newName)
      throws KeeperException, InterruptedException {
    Assert.assertNull("Node existed before created", zk.exists(newName, false));
    String path = zk.create(newName, newName.getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    Assert.assertEquals(path, newName);
    Assert.assertNotNull("Node was not created as expected",
                         zk.exists(newName, false));
  }

  private Stat createWithStatVerifyResult(String newName)
        throws KeeperException, InterruptedException {
    Assert.assertNull("Node existed before created", zk.exists(newName, false));
    Stat stat = new Stat();
    String path = zk.create(newName, newName.getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
    Assert.assertEquals(path, newName);
    validateCreateStat(stat, newName);

    Stat referenceStat = zk.exists(newName, false);
    Assert.assertNotNull("Node was not created as expected", referenceStat);
    Assert.assertEquals(referenceStat, stat);

    return stat;
  }

  private void validateCreateStat(Stat stat, String name) {
    Assert.assertEquals(stat.getCzxid(), stat.getMzxid());
    Assert.assertEquals(stat.getCzxid(), stat.getPzxid());
    Assert.assertEquals(stat.getCtime(), stat.getMtime());
    Assert.assertEquals(0, stat.getCversion());
    Assert.assertEquals(0, stat.getVersion());
    Assert.assertEquals(0, stat.getAversion());
    Assert.assertEquals(0, stat.getEphemeralOwner());
    Assert.assertEquals(name.length(), stat.getDataLength());
    Assert.assertEquals(0, stat.getNumChildren());
  }
}
