package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;

public class ConfCommand extends AbstractFourLetterCommand {
    ConfCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw,serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            zkServer.dumpConf(pw);
        }
    }
}
