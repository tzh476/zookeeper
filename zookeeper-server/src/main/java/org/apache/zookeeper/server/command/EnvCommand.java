package org.apache.zookeeper.server.command;

import java.io.PrintWriter;
import java.util.List;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.server.ServerCnxn;

public class EnvCommand extends AbstractFourLetterCommand {
    EnvCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        List<Environment.Entry> env = Environment.list();

        pw.println("Environment:");
        for (Environment.Entry e : env) {
            pw.print(e.getKey());
            pw.print("=");
            pw.println(e.getValue());
        }
    }
}
