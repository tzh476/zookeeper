package org.apache.zookeeper.server.quorum;

public class LearnerSnapshot {
    private final LearnerSnapshotThrottler throttler;
    private final int concurrentSnapshotNumber;
    private final boolean essential;

    LearnerSnapshot(LearnerSnapshotThrottler throttler, 
            int concurrentSnapshotNumber, boolean essential) {
        this.throttler = throttler;
        this.concurrentSnapshotNumber = concurrentSnapshotNumber;
        this.essential = essential;
    }

    public void close() {
        throttler.endSnapshot();
    }

    public int getConcurrentSnapshotNumber() {
        return concurrentSnapshotNumber;
    }
    
    public boolean isEssential() {
        return essential;
    }
}
