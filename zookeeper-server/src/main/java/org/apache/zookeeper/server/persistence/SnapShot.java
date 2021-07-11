package org.apache.zookeeper.server.persistence;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.server.DataTree;


public interface SnapShot {
    
    
    long deserialize(DataTree dt, Map<Long, Integer> sessions) 
        throws IOException;
    
    
    void serialize(DataTree dt, Map<Long, Integer> sessions, 
            File name) 
        throws IOException;
    
    
    File findMostRecentSnapshot() throws IOException;
    
    
    void close() throws IOException;
} 
