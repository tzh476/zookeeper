package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LearnerSnapshotThrottler {
    private static final Logger LOG =
            LoggerFactory.getLogger(LearnerSnapshotThrottler.class);

    private final Object snapCountSyncObject = new Object();
    private int snapsInProgress;

    private final int maxConcurrentSnapshots;
    private final long timeoutMillis;

    
    public LearnerSnapshotThrottler(int maxConcurrentSnapshots,
                                    long timeoutMillis) {
        if (timeoutMillis < 0) {
            String errorMsg = "timeout cannot be negative, was " + timeoutMillis;
            throw new IllegalArgumentException(errorMsg);
        }
        if (maxConcurrentSnapshots <= 0) {
            String errorMsg = "maxConcurrentSnapshots must be positive, was " +
                    maxConcurrentSnapshots;
            throw new IllegalArgumentException(errorMsg);
        }

        this.maxConcurrentSnapshots = maxConcurrentSnapshots;
        this.timeoutMillis = timeoutMillis;

        synchronized (snapCountSyncObject) {
            snapsInProgress = 0;
        }
    }

    public LearnerSnapshotThrottler(int maxConcurrentSnapshots) {
        this(maxConcurrentSnapshots, 0);
    }

    
    public LearnerSnapshot beginSnapshot(boolean essential)
            throws SnapshotThrottleException, InterruptedException {
        int snapshotNumber;

        synchronized (snapCountSyncObject) {
            if (!essential
                && timeoutMillis > 0
                && snapsInProgress >= maxConcurrentSnapshots) {
                long timestamp = Time.currentElapsedTime();
                do {
                    snapCountSyncObject.wait(timeoutMillis);
                } while (snapsInProgress >= maxConcurrentSnapshots
                         && timestamp + timeoutMillis < Time.currentElapsedTime());
            }

            if (essential || snapsInProgress < maxConcurrentSnapshots) {
                snapsInProgress++;
                snapshotNumber = snapsInProgress;
            } else {
                throw new SnapshotThrottleException(snapsInProgress + 1,
                                                    maxConcurrentSnapshots);
            }
        }

        return new LearnerSnapshot(this, snapshotNumber, essential);
    }

    
    public void endSnapshot() {
        int newCount;
        synchronized (snapCountSyncObject) {
            snapsInProgress--;
            newCount = snapsInProgress;
            snapCountSyncObject.notify();
        }

        if (newCount < 0) {
            String errorMsg =
                    "endSnapshot() called incorrectly; current snapshot count is "
                            + newCount;
            LOG.error(errorMsg);
        }
    }
}
