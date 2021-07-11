package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;

public class ConsCommand extends AbstractFourLetterCommand {
    public ConsCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            for (ServerCnxn c : factory.getConnections()) {
                c.dumpConnectionInfo(pw, false);
                pw.println();
            }
            pw.println();
        }
    }
}
