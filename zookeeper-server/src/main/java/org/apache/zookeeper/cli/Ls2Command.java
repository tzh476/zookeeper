package org.apache.zookeeper.cli;

import java.util.List;
import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


public class Ls2Command extends CliCommand {

    private static Options options = new Options();
    private String args[];
    
    public Ls2Command() {
        super("ls2", "path [watch]");
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
        err.println("'ls2' has been deprecated. "
                  + "Please use 'ls [-s] path' instead.");
        String path = args[1];
        boolean watch = args.length > 2;
        Stat stat = new Stat();
        List<String> children;
        try {
            children = zk.getChildren(path, watch, stat);
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        out.println(children);
        new StatPrinter(out).print(stat);
        return watch;
    }
}
