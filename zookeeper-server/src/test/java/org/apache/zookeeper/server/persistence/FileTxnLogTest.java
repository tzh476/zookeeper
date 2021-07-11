package org.apache.zookeeper.server.persistence;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

public class FileTxnLogTest  extends ZKTestCase {
  protected static final Logger LOG = LoggerFactory.getLogger(FileTxnLogTest.class);

  private static final int KB = 1024;

  @Test
  public void testInvalidPreallocSize() {
    Assert.assertEquals("file should not be padded",
      10 * KB, FilePadding.calculateFileSizeWithPadding(7 * KB, 10 * KB, 0));
    Assert.assertEquals("file should not be padded",
      10 * KB, FilePadding.calculateFileSizeWithPadding(7 * KB, 10 * KB, -1));
  }

  @Test
  public void testCalculateFileSizeWithPaddingWhenNotToCurrentSize() {
    Assert.assertEquals("file should not be padded",
      10 * KB, FilePadding.calculateFileSizeWithPadding(5 * KB, 10 * KB, 10 * KB));
  }

  @Test
  public void testCalculateFileSizeWithPaddingWhenCloseToCurrentSize() {
    Assert.assertEquals("file should be padded an additional 10 KB",
      20 * KB, FilePadding.calculateFileSizeWithPadding(7 * KB, 10 * KB, 10 * KB));
  }

  @Test
  public void testFileSizeGreaterThanPosition() {
    Assert.assertEquals("file should be padded to 40 KB",
      40 * KB, FilePadding.calculateFileSizeWithPadding(31 * KB, 10 * KB, 10 * KB));
  }

  @Test
  public void testPreAllocSizeSmallerThanTxnData() throws IOException {
    File logDir = ClientBase.createTmpDir();
    FileTxnLog fileTxnLog = new FileTxnLog(logDir);

        final int preAllocSize = 500 * KB;
    FilePadding.setPreallocSize(preAllocSize);

            byte[] data = new byte[2 * preAllocSize];
    Arrays.fill(data, (byte) 0xff);

            fileTxnLog.append(new TxnHeader(1, 1, 1, 1, ZooDefs.OpCode.create),
      new CreateTxn("/testPreAllocSizeSmallerThanTxnData1", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 0));
    fileTxnLog.commit();
    fileTxnLog.append(new TxnHeader(1, 1, 2, 2, ZooDefs.OpCode.create),
      new CreateTxn("/testPreAllocSizeSmallerThanTxnData2", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 0));
    fileTxnLog.commit();
    fileTxnLog.close();

        FileTxnLog.FileTxnIterator fileTxnIterator = new FileTxnLog.FileTxnIterator(logDir, 0);

        CreateTxn createTxn = (CreateTxn) fileTxnIterator.getTxn();
    Assert.assertTrue(Arrays.equals(createTxn.getData(), data));

        fileTxnIterator.next();
    createTxn = (CreateTxn) fileTxnIterator.getTxn();
    Assert.assertTrue(Arrays.equals(createTxn.getData(), new byte[]{}));
  }

  @Test
  public void testSetPreallocSize() {
    long customPreallocSize = 10101;
    FileTxnLog.setPreallocSize(customPreallocSize);
    Assert.assertThat(FilePadding.getPreAllocSize(), is(equalTo(customPreallocSize)));
  }

  public void testSyncThresholdExceedCount() throws IOException {
    
        java.lang.System.setProperty(FileTxnLog.ZOOKEEPER_FSYNC_WARNING_THRESHOLD_MS_PROPERTY, "-1");
    ServerStats.Provider providerMock = mock(ServerStats.Provider.class);
    ServerStats serverStats = new ServerStats(providerMock);

    File logDir = ClientBase.createTmpDir();
    FileTxnLog fileTxnLog = new FileTxnLog(logDir);
    fileTxnLog.setServerStats(serverStats);

        Assert.assertEquals(0L, serverStats.getFsyncThresholdExceedCount());

        for (int i = 0; i < 50; i++) {
      fileTxnLog.append(new TxnHeader(1, 1, 1, 1, ZooDefs.OpCode.create),
              new CreateTxn("/testFsyncThresholdCountIncreased", new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 0));
      fileTxnLog.commit();             Assert.assertEquals((long) i + 1 , serverStats.getFsyncThresholdExceedCount());
    }
  }
}
