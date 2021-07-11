package org.apache.zookeeper.server;


public enum ExitCode {

    
    EXECUTION_FINISHED(0),

    
    UNEXPECTED_ERROR(1),

    
    INVALID_INVOCATION(2),

    
    UNABLE_TO_ACCESS_DATADIR(3),

    
    ERROR_STARTING_ADMIN_SERVER(4),

    
    TXNLOG_ERROR_TAKING_SNAPSHOT(10),

    
    UNMATCHED_TXN_COMMIT(12),

    
    QUORUM_PACKET_ERROR(13),

    
    UNABLE_TO_BIND_QUORUM_PORT(14);

    private final int value;

    ExitCode(final int newValue) {
        value = newValue;
    }

    public int getValue() {
        return value;
    }

}