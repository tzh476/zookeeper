package org.apache.zookeeper.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;


public class DeleteAllCommand extends CliCommand {

    private static Options options = new Options();
    private String[] args;

    public DeleteAllCommand() {
        this("deleteall");
    }

    public DeleteAllCommand(String cmdStr) {
        super(cmdStr, "path");
    }
    
    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        Parser parser = new PosixParser();
        CommandLine cl;
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
    public boolean exec() throws CliException {
        printDeprecatedWarning();
        
        String path = args[1];
        try {
            ZKUtil.deleteRecursive(zk, path);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        return false;
    }
    
    private void printDeprecatedWarning() {
        if("rmr".equals(args[0])) {
            err.println("The command 'rmr' has been deprecated. " +
                  "Please use 'deleteall' instead.");
        }
    }
}
