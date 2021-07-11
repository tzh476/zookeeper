package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;

public class DumpCommand extends AbstractFourLetterCommand {
    public DumpCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            pw.println("SessionTracker dump:");
            zkServer.getSessionTracker().dumpSessions(pw);
            pw.println("ephemeral nodes dump:");
            zkServer.dumpEphemerals(pw);
            pw.println("Connections dump:");
                        if (factory instanceof NIOServerCnxnFactory) {
                ((NIOServerCnxnFactory)factory).dumpConnections(pw);
            }
        }
    }
}
