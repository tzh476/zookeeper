package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;

public class CnxnStatResetCommand extends AbstractFourLetterCommand {
    public CnxnStatResetCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            factory.resetAllConnectionStats();
            pw.println("Connection stats reset.");
        }
    }
}
