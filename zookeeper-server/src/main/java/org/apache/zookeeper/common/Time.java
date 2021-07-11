package org.apache.zookeeper.common;

import java.util.Date;

public class Time {
    
    public static long currentElapsedTime() {
        return System.nanoTime() / 1000000;
    }

    
    public static long currentWallTime() {
        return System.currentTimeMillis();
    }

    
    public static Date elapsedTimeToDate(long elapsedTime) {
        long wallTime = currentWallTime() + elapsedTime - currentElapsedTime();
        return new Date(wallTime);
    }
}