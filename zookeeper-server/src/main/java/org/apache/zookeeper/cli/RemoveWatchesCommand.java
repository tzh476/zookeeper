package org.apache.zookeeper.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.WatcherType;


public class RemoveWatchesCommand extends CliCommand {

    private static Options options = new Options();
    private String[] args;
    private CommandLine cl;

    static {
        options.addOption("c", false, "child watcher type");
        options.addOption("d", false, "data watcher type");
        options.addOption("a", false, "any watcher type");
        options.addOption("l", false,
                "remove locally when there is no server connection");
    }

    public RemoveWatchesCommand() {
        super("removewatches", "path [-c|-d|-a] [-l]");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        try {
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }
        args = cl.getArgs();
        if (args.length < 2) {
            throw new CliParseException(getUsageStr());
        }
        return this;
    }

    @Override
    public boolean exec() throws CliWrapperException, MalformedPathException {
        String path = args[1];
        WatcherType wtype = WatcherType.Any;
                        if (cl.hasOption("c")) {
            wtype = WatcherType.Children;
        } else if (cl.hasOption("d")) {
            wtype = WatcherType.Data;
        } else if (cl.hasOption("a")) {
            wtype = WatcherType.Any;
        }
                boolean local = cl.hasOption("l");

        try {
            zk.removeAllWatches(path, wtype, local);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        return true;
    }
}
