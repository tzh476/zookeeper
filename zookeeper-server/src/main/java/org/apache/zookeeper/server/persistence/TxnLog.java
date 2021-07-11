package org.apache.zookeeper.server.persistence;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.txn.TxnHeader;


public interface TxnLog {

    
    void setServerStats(ServerStats serverStats);
    
    
    void rollLog() throws IOException;
    
    boolean append(TxnHeader hdr, Record r) throws IOException;

    
    TxnIterator read(long zxid) throws IOException;
    
    
    long getLastLoggedZxid() throws IOException;
    
    
    boolean truncate(long zxid) throws IOException;
    
    
    long getDbId() throws IOException;
    
    
    void commit() throws IOException;

    
    long getTxnLogSyncElapsedTime();
   
    
    void close() throws IOException;
    
    public interface TxnIterator {
        
        TxnHeader getHeader();
        
        
        Record getTxn();
     
        
        boolean next() throws IOException;
        
        
        void close() throws IOException;
        
        
        long getStorageSize() throws IOException;
    }
}

