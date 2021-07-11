package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;


public class DeleteCommand extends CliCommand {

    private static Options options = new Options();
    private String[] args;
    private CommandLine cl;

    static {
        options.addOption("v", true, "version");
    }

    public DeleteCommand() {
        super("delete", "[-v version] path");
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
            err.println("'delete path [version]' has been deprecated. "
                    + "Please use 'delete [-v version] path' instead.");
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
        int version;
        if (cl.hasOption("v")) {
            version = Integer.parseInt(cl.getOptionValue("v"));
        } else {
            version = -1;
        }
        
        try {
            zk.delete(path, version);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch(KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        return false;
    }
}
