package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;

public class TraceMaskCommand extends AbstractFourLetterCommand {
    TraceMaskCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        long traceMask = ZooTrace.getTextTraceLevel();
        pw.print(traceMask);
    }
}
