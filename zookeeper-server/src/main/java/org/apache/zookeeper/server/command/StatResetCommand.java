package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;

public class StatResetCommand extends AbstractFourLetterCommand {
    public StatResetCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            ServerStats serverStats = zkServer.serverStats();
            serverStats.reset();
            if (serverStats.getServerState().equals("leader")) {
                ((LeaderZooKeeperServer)zkServer).getLeader().getProposalStats().reset();
            }
            pw.println("Server stats reset.");
        }
    }
}
