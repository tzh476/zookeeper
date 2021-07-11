package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ServerCnxn;

public class WatchCommand extends AbstractFourLetterCommand {
    int len = 0;
    public WatchCommand(PrintWriter pw, ServerCnxn serverCnxn, int len) {
        super(pw, serverCnxn);
        this.len = len;
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
        } else {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            if (len == FourLetterCommands.wchsCmd) {
                dt.dumpWatchesSummary(pw);
            } else if (len == FourLetterCommands.wchpCmd) {
                dt.dumpWatches(pw, true);
            } else {
                dt.dumpWatches(pw, false);
            }
            pw.println();
        }
    }
}
