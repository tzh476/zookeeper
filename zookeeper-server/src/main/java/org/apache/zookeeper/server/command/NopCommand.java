package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;


public class NopCommand extends AbstractFourLetterCommand {
    private String msg;

    public NopCommand(PrintWriter pw, ServerCnxn serverCnxn, String msg) {
        super(pw, serverCnxn);
        this.msg = msg;
    }

    @Override
    public void commandRun() {
        pw.println(msg);
    }
}
