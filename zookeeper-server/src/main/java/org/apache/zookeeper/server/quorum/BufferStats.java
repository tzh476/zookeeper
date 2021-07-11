package org.apache.zookeeper.server.quorum;


public class BufferStats {
    public static final int INIT_VALUE = -1;

    
    private int lastBufferSize = INIT_VALUE;

    
    private int minBufferSize = INIT_VALUE;

    
    private int maxBufferSize = INIT_VALUE;

    
    public synchronized int getLastBufferSize() {
        return lastBufferSize;
    }

    
    public synchronized void setLastBufferSize(int value) {
        lastBufferSize = value;
        if (minBufferSize == INIT_VALUE || value < minBufferSize) {
            minBufferSize = value;
        }
        if (value > maxBufferSize) {
            maxBufferSize = value;
        }
    }

    
    public synchronized int getMinBufferSize() {
        return minBufferSize;
    }

    
    public synchronized int getMaxBufferSize() {
        return maxBufferSize;
    }

    
    public synchronized void reset() {
        lastBufferSize = INIT_VALUE;
        minBufferSize = INIT_VALUE;
        maxBufferSize = INIT_VALUE;
    }

    @Override
    public synchronized String toString() {
        return String.format("%d/%d/%d", lastBufferSize, minBufferSize, maxBufferSize);
    }
}
