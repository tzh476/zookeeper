package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.util.ConfigUtils;


public class GetConfigCommand extends CliCommand {

    private static Options options = new Options();
    private String args[];
    private CommandLine cl;

    static {
        options.addOption("s", false, "stats");
        options.addOption("w", false, "watch");
        options.addOption("c", false, "client connection string");
    }

    public GetConfigCommand() {
        super("config", "[-c] [-w] [-s]");
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
        if (args.length < 1) {
            throw new CliParseException(getUsageStr());
        }

        return this;
    }

    @Override
    public boolean exec() throws CliException {
        boolean watch = cl.hasOption("w");        
        Stat stat = new Stat();
        byte data[];
        try {
            data = zk.getConfig(watch, stat);
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        data = (data == null) ? "null".getBytes() : data;
        if (cl.hasOption("c")) {
            out.println(ConfigUtils.getClientConfigStr(new String(data)));
        } else {
            out.println(new String(data));
        }
        
        if (cl.hasOption("s")) {
            new StatPrinter(out).print(stat);
        }                
        
        return watch;
    }
}
