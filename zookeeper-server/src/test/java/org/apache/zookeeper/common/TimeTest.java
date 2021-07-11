package org.apache.zookeeper.common;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;


public class TimeTest extends ClientBase {
    private static final long mt0 = System.currentTimeMillis();
    private static final long nt0 = Time.currentElapsedTime();

    private static AtomicInteger watchCount = new AtomicInteger(0);


    public static void main(String[] args) throws Exception {
        System.out.printf("Starting\n");
        final TimeTest test = new TimeTest();
        System.out.printf("After construct\n");
        test.setUp();
        ZooKeeper zk = test.createClient();
        zk.create("/ephemeral", new byte[]{1, 2, 3},
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while (Time.currentElapsedTime() - nt0 < 100000) {
            System.out.printf("%d\t%s\n", discrepancy(),
                    zk.exists("/ephemeral",
                            watchCount.get() == 0 ? createWatcher() : null) != null);
            waitByYielding(500);
        }
    }

    private static Watcher createWatcher() {
        watchCount.incrementAndGet();
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                watchCount.decrementAndGet();
                System.out.printf("%d event = %s\n", discrepancy(), event);
            }
        };

    }

    private static void waitByYielding(long delay) {
        long t0 = Time.currentElapsedTime();
        while (Time.currentElapsedTime() < t0 + delay) {
            Thread.yield();
        }
    }

    private static long discrepancy() {
        return (System.currentTimeMillis() - mt0) - (Time.currentElapsedTime() - nt0);
    }

    @Test
    public void testElapsedTimeToDate() throws Exception {
        long walltime = Time.currentWallTime();
        long elapsedTime = Time.currentElapsedTime();
        Thread.sleep(200);

        Calendar cal = Calendar.getInstance();
        cal.setTime(Time.elapsedTimeToDate(elapsedTime));
        int calculatedDate = cal.get(Calendar.HOUR_OF_DAY);
        cal.setTime(new Date(walltime));
        int realDate = cal.get(Calendar.HOUR_OF_DAY);

        Assert.assertEquals(calculatedDate, realDate);
    }
}
