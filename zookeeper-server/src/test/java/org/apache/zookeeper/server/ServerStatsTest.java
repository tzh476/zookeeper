package org.apache.zookeeper.server;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ServerStatsTest extends ZKTestCase {

    private ServerStats.Provider providerMock;

    @Before
    public void setUp() {
        providerMock = mock(ServerStats.Provider.class);
    }

    @Test
    public void testPacketsMetrics() {
                ServerStats serverStats = new ServerStats(providerMock);
        int incrementCount = 20;

                for (int i = 0; i < incrementCount; i++) {
            serverStats.incrementPacketsSent();
            serverStats.incrementPacketsReceived();
            serverStats.incrementPacketsReceived();
        }

                Assert.assertEquals(incrementCount, serverStats.getPacketsSent());
        Assert.assertEquals(incrementCount*2, serverStats.getPacketsReceived());

                serverStats.resetRequestCounters();

                assertAllPacketsZero(serverStats);

    }

    @Test
    public void testLatencyMetrics() {
                ServerStats serverStats = new ServerStats(providerMock);

                serverStats.updateLatency(Time.currentElapsedTime()-1000);
        serverStats.updateLatency(Time.currentElapsedTime()-2000);

                assertThat("Max latency check", 2000L,
                lessThanOrEqualTo(serverStats.getMaxLatency()));
        assertThat("Min latency check", 1000L,
                lessThanOrEqualTo(serverStats.getMinLatency()));
        assertThat("Avg latency check", 1500L,
                lessThanOrEqualTo(serverStats.getAvgLatency()));

                serverStats.resetLatency();

                assertAllLatencyZero(serverStats);
    }

    @Test
    public void testFsyncThresholdExceedMetrics() {
                ServerStats serverStats = new ServerStats(providerMock);
        int incrementCount = 30;

                for (int i = 0; i < incrementCount; i++) {
            serverStats.incrementFsyncThresholdExceedCount();
        }

                Assert.assertEquals(incrementCount, serverStats.getFsyncThresholdExceedCount());

                serverStats.resetFsyncThresholdExceedCount();

                assertFsyncThresholdExceedCountZero(serverStats);

    }

    @Test
    public void testReset() {
                ServerStats serverStats = new ServerStats(providerMock);

        assertAllPacketsZero(serverStats);
        assertAllLatencyZero(serverStats);

                serverStats.incrementPacketsSent();
        serverStats.incrementPacketsReceived();
        serverStats.updateLatency(Time.currentElapsedTime()-1000);

        serverStats.reset();

                assertAllPacketsZero(serverStats);
        assertAllLatencyZero(serverStats);
    }

    private void assertAllPacketsZero(ServerStats serverStats) {
        Assert.assertEquals(0L, serverStats.getPacketsSent());
        Assert.assertEquals(0L, serverStats.getPacketsReceived());
    }

    private void assertAllLatencyZero(ServerStats serverStats) {
        Assert.assertEquals(0L, serverStats.getMaxLatency());
        Assert.assertEquals(0L, serverStats.getMinLatency());
        Assert.assertEquals(0L, serverStats.getAvgLatency());
    }

    private void assertFsyncThresholdExceedCountZero(ServerStats serverStats) {
        Assert.assertEquals(0L, serverStats.getFsyncThresholdExceedCount());
    }
}
