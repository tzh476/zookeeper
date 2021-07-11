package org.apache.zookeeper;


public class StatsTrack {
    private int count;
    private long bytes;
    private String countStr = "count";
    private String byteStr = "bytes";

    
    public StatsTrack() {
        this(null);
    }
    
    public StatsTrack(String stats) {
        if (stats == null) {
            stats = "count=-1,bytes=-1";
        }
        String[] split = stats.split(",");
        if (split.length != 2) {
            throw new IllegalArgumentException("invalid string " + stats);
        }
        count = Integer.parseInt(split[0].split("=")[1]);
        bytes = Long.parseLong(split[1].split("=")[1]);
    }


    
    public int getCount() {
        return this.count;
    }

    
    public void setCount(int count) {
        this.count = count;
    }

    
    public long getBytes() {
        return this.bytes;
    }

    
    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    @Override
    
    public String toString() {
        return countStr + "=" + count + "," + byteStr + "=" + bytes;
    }
}
