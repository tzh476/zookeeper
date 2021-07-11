package org.apache.zookeeper.cli;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.data.Stat;


public class ListQuotaCommand extends CliCommand {

    private static Options options = new Options();
    private String[] args;
    
    public ListQuotaCommand() {
        super("listquota", "path");
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
        if(args.length < 2) {
            throw new CliParseException(getUsageStr());
        }
        
        return this;
    }
    
    @Override
    public boolean exec() throws CliException {
        String path = args[1];
        String absolutePath = Quotas.quotaZookeeper + path + "/"
                + Quotas.limitNode;
        try {
            err.println("absolute path is " + absolutePath);
            Stat stat = new Stat();
            byte[] data = zk.getData(absolutePath, false, stat);
            StatsTrack st = new StatsTrack(new String(data));
            out.println("Output quota for " + path + " "
                    + st.toString());

            data = zk.getData(Quotas.quotaZookeeper + path + "/"
                    + Quotas.statNode, false, stat);
            out.println("Output stat for " + path + " "
                    + new StatsTrack(new String(data)).toString());
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException.NoNodeException ne) {
            err.println("quota for " + path + " does not exist.");
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        
        return false;
    }
}
