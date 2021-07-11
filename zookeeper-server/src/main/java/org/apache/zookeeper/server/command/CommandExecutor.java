package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class CommandExecutor {
    
    public boolean execute(ServerCnxn serverCnxn, PrintWriter pwriter,
            final int commandCode, ZooKeeperServer zkServer, ServerCnxnFactory factory) {
        AbstractFourLetterCommand command = getCommand(serverCnxn,pwriter, commandCode);

        if (command == null) {
            return false;
        }

        command.setZkServer(zkServer);
        command.setFactory(factory);
        command.start();
        return true;
    }

    private AbstractFourLetterCommand getCommand(ServerCnxn serverCnxn,
            PrintWriter pwriter, final int commandCode) {
        AbstractFourLetterCommand command = null;
        if (commandCode == FourLetterCommands.ruokCmd) {
            command = new RuokCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.getTraceMaskCmd) {
            command = new TraceMaskCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.enviCmd) {
            command = new EnvCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.confCmd) {
            command = new ConfCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.srstCmd) {
            command = new StatResetCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.crstCmd) {
            command = new CnxnStatResetCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.dirsCmd) {
            command = new DirsCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.dumpCmd) {
            command = new DumpCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.statCmd
                || commandCode == FourLetterCommands.srvrCmd) {
            command = new StatCommand(pwriter, serverCnxn, commandCode);
        } else if (commandCode == FourLetterCommands.consCmd) {
            command = new ConsCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.wchpCmd
                || commandCode == FourLetterCommands.wchcCmd
                || commandCode == FourLetterCommands.wchsCmd) {
            command = new WatchCommand(pwriter, serverCnxn, commandCode);
        } else if (commandCode == FourLetterCommands.mntrCmd) {
            command = new MonitorCommand(pwriter, serverCnxn);
        } else if (commandCode == FourLetterCommands.isroCmd) {
            command = new IsroCommand(pwriter, serverCnxn);
        }
        return command;
    }

}
