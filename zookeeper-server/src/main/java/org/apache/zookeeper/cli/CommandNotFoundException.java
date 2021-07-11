package org.apache.zookeeper.cli;

@SuppressWarnings("serial")
public class CommandNotFoundException extends CliException {

    public CommandNotFoundException(String command) {
        super("Command not found: " + command, 127);
    }
}
