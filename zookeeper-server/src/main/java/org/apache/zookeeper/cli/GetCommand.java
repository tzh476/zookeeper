package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


public class GetCommand extends CliCommand {

    private static Options options = new Options();
    private String args[];
    private CommandLine cl;

    static {
        options.addOption("s", false, "stats");
        options.addOption("w", false, "watch");
    }

    public GetCommand() {
        super("get", "[-s] [-w] path");
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

        retainCompatibility(cmdArgs);

        return this;
    }

    private void retainCompatibility(String[] cmdArgs) throws CliParseException {
                if (args.length > 2) {
                        cmdArgs[2] = "-w";
            err.println("'get path [watch]' has been deprecated. "
                    + "Please use 'get [-s] [-w] path' instead.");
            Parser parser = new PosixParser();
            try {
                cl = parser.parse(options, cmdArgs);
            } catch (ParseException ex) {
                throw new CliParseException(ex);
            }
            args = cl.getArgs();
        }
    }

    @Override
    public boolean exec() throws CliException {
        boolean watch = cl.hasOption("w");
        String path = args[1];
        Stat stat = new Stat();
        byte data[];
        try {
            data = zk.getData(path, watch, stat);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException|InterruptedException ex) {
            throw new CliException(ex);
        }
        data = (data == null) ? "null".getBytes() : data;
        out.println(new String(data));
        if (cl.hasOption("s")) {
            new StatPrinter(out).print(stat);
        }
        return watch;
    }
}
