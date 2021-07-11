package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;

public class IsroCommand extends AbstractFourLetterCommand {

    public IsroCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.print("null");
        } else if (zkServer instanceof ReadOnlyZooKeeperServer) {
            pw.print("ro");
        } else {
            pw.print("rw");
        }
    }
}
