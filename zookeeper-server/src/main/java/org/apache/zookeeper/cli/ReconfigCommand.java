package org.apache.zookeeper.cli;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;


public class ReconfigCommand extends CliCommand {

    private static Options options = new Options();

    
    private String joining;

    
    private String leaving;

    
    private String members;

    
    long version = -1;
    private CommandLine cl;

    static {
        options.addOption("s", false, "stats");
        options.addOption("v", true, "required current config version");
        options.addOption("file", true, "path of config file to parse for membership");
        options.addOption("members", true, "comma-separated list of config strings for " +
        		"non-incremental reconfig");
        options.addOption("add", true, "comma-separated list of config strings for " +
        		"new servers");
        options.addOption("remove", true, "comma-separated list of server IDs to remove");
    }

    public ReconfigCommand() {
        super("reconfig", "[-s] " +
        		"[-v version] " +
        		"[[-file path] | " +
        		"[-members serverID=host:port1:port2;port3[,...]*]] | " +
        		"[-add serverId=host:port1:port2;port3[,...]]* " +
        		"[-remove serverId[,...]*]");
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        joining = null;
        leaving = null;
        members = null;
        Parser parser = new PosixParser();
        try {
            cl = parser.parse(options, cmdArgs);
        } catch (ParseException ex) {
            throw new CliParseException(ex);
        }
        if (!(cl.hasOption("file") || cl.hasOption("members")) && !cl.hasOption("add") && !cl.hasOption("remove")) {
            throw new CliParseException(getUsageStr());
        }
        if (cl.hasOption("v")) {
            try{ 
                version = Long.parseLong(cl.getOptionValue("v"), 16);
            } catch (NumberFormatException e){
                throw new CliParseException("-v must be followed by a long (configuration version)");
            }
        } else {
            version = -1;
        }

                if ((cl.hasOption("file") || cl.hasOption("members")) && (cl.hasOption("add") || cl.hasOption("remove"))) {
            throw new CliParseException("Can't use -file or -members together with -add or -remove (mixing incremental" +
            		" and non-incremental modes is not allowed)");
        }
        if (cl.hasOption("file") && cl.hasOption("members")) {
            throw new CliParseException("Can't use -file and -members together (conflicting non-incremental modes)");
        }

                if (cl.hasOption("add")) {
           joining = cl.getOptionValue("add").toLowerCase();
        }
        if (cl.hasOption("remove")) {
           leaving = cl.getOptionValue("remove").toLowerCase();
        }
        if (cl.hasOption("members")) {
           members = cl.getOptionValue("members").toLowerCase();
        }
        if (cl.hasOption("file")) {
            try {
                Properties dynamicCfg = new Properties();
                try (FileInputStream inConfig = new FileInputStream(cl.getOptionValue("file"))) {
                    dynamicCfg.load(inConfig);
                }
                                                                members = QuorumPeerConfig.parseDynamicConfig(dynamicCfg, 0, true, false).toString();
            } catch (Exception e) {
                throw new CliParseException("Error processing " + cl.getOptionValue("file") + e.getMessage());
            } 
        }
        return this;
    }

    @Override
    public boolean exec() throws CliException {
        try {
            Stat stat = new Stat();
            if (!(zk instanceof ZooKeeperAdmin)) {
                                                                                                return false;
            }

            byte[] curConfig = ((ZooKeeperAdmin)zk).reconfigure(joining,
                    leaving, members, version, stat);
            out.println("Committed new configuration:\n" + new String(curConfig));
            
            if (cl.hasOption("s")) {
                new StatPrinter(out).print(stat);
            }
        } catch (KeeperException|InterruptedException ex) {
            throw new CliWrapperException(ex);
        }
        return false;
    }
}
