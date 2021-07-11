package org.apache.zookeeper.server.quorum;


public class SnapshotThrottleException extends Exception {
    private static final long serialVersionUID = 1L;

    public SnapshotThrottleException(int concurrentSnapshotNumber, int throttleThreshold) {
        super(getMessage(concurrentSnapshotNumber, throttleThreshold));
    }

    private static String getMessage(int concurrentSnapshotNumber, int throttleThreshold) {
        return String.format("new snapshot would make %d concurrently in progress; " +
                "maximum is %d", concurrentSnapshotNumber, throttleThreshold);
    }
}
