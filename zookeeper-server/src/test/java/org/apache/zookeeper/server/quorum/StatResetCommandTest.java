package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.command.StatResetCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.apache.zookeeper.server.command.AbstractFourLetterCommand.ZK_NOT_SERVING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatResetCommandTest {
    private StatResetCommand statResetCommand;
    private StringWriter outputWriter;
    private ZooKeeperServer zks;
    private ServerStats serverStats;

    @Before
    public void setUp() {
        outputWriter = new StringWriter();
        ServerCnxn serverCnxnMock = mock(ServerCnxn.class);

        zks = mock(ZooKeeperServer.class);
        when(zks.isRunning()).thenReturn(true);

        serverStats = mock(ServerStats.class);
        when(zks.serverStats()).thenReturn(serverStats);

        statResetCommand = new StatResetCommand(new PrintWriter(outputWriter), serverCnxnMock);
        statResetCommand.setZkServer(zks);
    }

    @Test
    public void testStatResetWithZKNotRunning() {
                when(zks.isRunning()).thenReturn(false);

                statResetCommand.commandRun();

                String output = outputWriter.toString();
        assertEquals(ZK_NOT_SERVING + "\n", output);
    }

    @Test
    public void testStatResetWithFollower() {
                when(zks.isRunning()).thenReturn(true);
        when(serverStats.getServerState()).thenReturn("follower");

                statResetCommand.commandRun();

                String output = outputWriter.toString();
        assertEquals("Server stats reset.\n", output);
        verify(serverStats, times(1)).reset();
    }

    @Test
    public void testStatResetWithLeader() {
                LeaderZooKeeperServer leaderZks = mock(LeaderZooKeeperServer.class);
        when(leaderZks.isRunning()).thenReturn(true);
        when(leaderZks.serverStats()).thenReturn(serverStats);
        Leader leader = mock(Leader.class);
        when(leaderZks.getLeader()).thenReturn(leader);
        statResetCommand.setZkServer(leaderZks);

        when(serverStats.getServerState()).thenReturn("leader");

        BufferStats bufferStats = mock(BufferStats.class);
        when(leader.getProposalStats()).thenReturn(bufferStats);

                statResetCommand.commandRun();

                String output = outputWriter.toString();
        assertEquals("Server stats reset.\n", output);
        verify(serverStats, times(1)).reset();
        verify(bufferStats, times(1)).reset();
    }
}
