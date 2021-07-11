package org.apache.zookeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class MultiAsyncTransactionTest extends ClientBase {
    private ZooKeeper zk;
    private final AtomicInteger pendingOps = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        zk = createClient();
        pendingOps.set(0);
    }

    private static class MultiResult {
        int rc;
        List<OpResult> results;
    }

    private void finishPendingOps() {
        if (pendingOps.decrementAndGet() == 0) {
            synchronized (pendingOps) {
                pendingOps.notifyAll();
            }
        }
    }

    private void waitForPendingOps(int timeout) throws Exception {
        synchronized(pendingOps) {
            while(pendingOps.get() > 0) {
                pendingOps.wait(timeout);
            }
        }
    }

    
    @Test
    public void testSequentialNodeCreateInAsyncMulti() throws Exception {
        final int iteration = 4;
        final List<MultiResult> results = new ArrayList<MultiResult>();

        pendingOps.set(iteration);

        List<Op> ops = Arrays.asList(
                Op.create("/node-", new byte[0], Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT_SEQUENTIAL),
                Op.create("/dup", new byte[0], Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT));


        for (int i = 0; i < iteration; ++i) {
            zk.multi(ops, new MultiCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx,
                        List<OpResult> opResults) {
                    MultiResult result = new MultiResult();
                    result.results = opResults;
                    result.rc = rc;
                    results.add(result);
                    finishPendingOps();
                }
            }, null);
        }

        waitForPendingOps(CONNECTION_TIMEOUT);

                assertEquals(KeeperException.Code.OK.intValue(), results.get(0).rc);
        assertEquals(KeeperException.Code.NODEEXISTS.intValue(), results.get(1).rc);
        assertEquals(KeeperException.Code.NODEEXISTS.intValue(), results.get(2).rc);
        assertEquals(KeeperException.Code.NODEEXISTS.intValue(), results.get(3).rc);

                assertTrue(results.get(0).results.get(0) instanceof CreateResult);
        assertEquals(KeeperException.Code.OK.intValue(),
                ((ErrorResult) results.get(1).results.get(0)).getErr());
        assertEquals(KeeperException.Code.OK.intValue(),
                ((ErrorResult) results.get(2).results.get(0)).getErr());
        assertEquals(KeeperException.Code.OK.intValue(),
                ((ErrorResult) results.get(3).results.get(0)).getErr());

                assertEquals(KeeperException.Code.NODEEXISTS.intValue(),
                ((ErrorResult) results.get(1).results.get(1)).getErr());
        assertEquals(KeeperException.Code.NODEEXISTS.intValue(),
                ((ErrorResult) results.get(2).results.get(1)).getErr());
        assertEquals(KeeperException.Code.NODEEXISTS.intValue(),
                ((ErrorResult) results.get(3).results.get(1)).getErr());

    }
}