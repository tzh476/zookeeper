package org.apache.zookeeper.server.command;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;

public class DirsCommand extends AbstractFourLetterCommand {

    public DirsCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() throws IOException {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
            return;
        }
        pw.println("datadir_size: " + zkServer.getDataDirSize());
        pw.println("logdir_size: " + zkServer.getLogDirSize());
    }
}
