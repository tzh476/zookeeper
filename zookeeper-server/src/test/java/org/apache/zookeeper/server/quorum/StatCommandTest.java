package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.StatCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatCommandTest {
    private StringWriter outputWriter;
    private StatCommand statCommand;
    private ServerStats.Provider providerMock;

    @Before
    public void setUp() {
        outputWriter = new StringWriter();
        ServerCnxn serverCnxnMock = mock(ServerCnxn.class);

        LeaderZooKeeperServer zks = mock(LeaderZooKeeperServer.class);
        when(zks.isRunning()).thenReturn(true);
        providerMock = mock(ServerStats.Provider.class);
        when(zks.serverStats()).thenReturn(new ServerStats(providerMock));
        ZKDatabase zkDatabaseMock = mock(ZKDatabase.class);
        when(zks.getZKDatabase()).thenReturn(zkDatabaseMock);
        Leader leaderMock = mock(Leader.class);
        when(leaderMock.getProposalStats()).thenReturn(new BufferStats());
        when(zks.getLeader()).thenReturn(leaderMock);

        ServerCnxnFactory serverCnxnFactory = mock(ServerCnxnFactory.class);
        ServerCnxn serverCnxn = mock(ServerCnxn.class);
        List<ServerCnxn> connections = new ArrayList<>();
        connections.add(serverCnxn);
        when(serverCnxnFactory.getConnections()).thenReturn(connections);

        statCommand = new StatCommand(new PrintWriter(outputWriter), serverCnxnMock, FourLetterCommands.statCmd);
        statCommand.setZkServer(zks);
        statCommand.setFactory(serverCnxnFactory);
    }

    @Test
    public void testLeaderStatCommand() {
                when(providerMock.getState()).thenReturn("leader");

                statCommand.commandRun();

                String output = outputWriter.toString();
        assertCommonStrings(output);
        assertThat(output, containsString("Mode: leader"));
        assertThat(output, containsString("Proposal sizes last/min/max:"));
    }

    @Test
    public void testFollowerStatCommand() {
                when(providerMock.getState()).thenReturn("follower");

                statCommand.commandRun();

                String output = outputWriter.toString();
        assertCommonStrings(output);
        assertThat(output, containsString("Mode: follower"));
    }

    private void assertCommonStrings(String output) {
        assertThat(output, containsString("Clients:"));
        assertThat(output, containsString("Zookeeper version:"));
        assertThat(output, containsString("Node count:"));
    }
}
