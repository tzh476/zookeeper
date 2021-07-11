package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;

public class SetTraceMaskCommand extends AbstractFourLetterCommand {
    long trace = 0;
    public SetTraceMaskCommand(PrintWriter pw, ServerCnxn serverCnxn, long trace) {
        super(pw, serverCnxn);
        this.trace = trace;
    }

    @Override
    public void commandRun() {
        pw.print(trace);
    }
}
