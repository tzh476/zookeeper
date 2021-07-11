package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


public class StatCommand extends CliCommand {

    private static final Options options = new Options();
    private String[] args;
    private CommandLine cl;

    static {
        options.addOption("w", false, "watch");
    }
    
    public StatCommand() {
        super("stat", "[-w] path");
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
        if(args.length < 2) {
            throw new CliParseException(getUsageStr());
        }    
        
        retainCompatibility(cmdArgs);

        return this;
    }

    private void retainCompatibility(String[] cmdArgs) throws CliParseException {
                if (args.length > 2) {
                        cmdArgs[2] = "-w";
            err.println("'stat path [watch]' has been deprecated. "
                    + "Please use 'stat [-w] path' instead.");
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
        String path = args[1];
        boolean watch = cl.hasOption("w");
        Stat stat;
        try {
            stat = zk.exists(path, watch);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        if (stat == null) {
            throw new CliWrapperException(new KeeperException.NoNodeException(path));
        }
        new StatPrinter(out).print(stat);
        return watch;
    }
}
