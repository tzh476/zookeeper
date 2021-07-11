package org.apache.zookeeper.server.quorum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BufferStatsTest {
    @Test
    public void testSetProposalSizeSetMinMax() {
        BufferStats stats = new BufferStats();
        assertEquals(-1, stats.getLastBufferSize());
        assertEquals(-1, stats.getMinBufferSize());
        assertEquals(-1, stats.getMaxBufferSize());
        stats.setLastBufferSize(10);
        assertEquals(10, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(10, stats.getMaxBufferSize());
        stats.setLastBufferSize(20);
        assertEquals(20, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(20, stats.getMaxBufferSize());
        stats.setLastBufferSize(5);
        assertEquals(5, stats.getLastBufferSize());
        assertEquals(5, stats.getMinBufferSize());
        assertEquals(20, stats.getMaxBufferSize());
    }

    @Test
    public void testReset() {
        BufferStats stats = new BufferStats();
        stats.setLastBufferSize(10);
        assertEquals(10, stats.getLastBufferSize());
        assertEquals(10, stats.getMinBufferSize());
        assertEquals(10, stats.getMaxBufferSize());
        stats.reset();
        assertEquals(-1, stats.getLastBufferSize());
        assertEquals(-1, stats.getMinBufferSize());
        assertEquals(-1, stats.getMaxBufferSize());
    }
}
