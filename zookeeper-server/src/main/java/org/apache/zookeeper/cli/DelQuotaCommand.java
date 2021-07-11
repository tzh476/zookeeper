package org.apache.zookeeper.cli;

import java.io.IOException;
import java.util.List;
import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class DelQuotaCommand extends CliCommand {

    private Options options = new Options();
    private String[] args;
    private CommandLine cl;

    public DelQuotaCommand() {
        super("delquota", "[-n|-b] path");

        OptionGroup og1 = new OptionGroup();
        og1.addOption(new Option("b", false, "bytes quota"));
        og1.addOption(new Option("n", false, "num quota"));
        options.addOptionGroup(og1);
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
    public boolean exec() throws CliException {
                        String path = args[1];
        try {
            if (cl.hasOption("b")) {
                delQuota(zk, path, true, false);
            } else if (cl.hasOption("n")) {
                delQuota(zk, path, false, true);
            } else if (args.length == 2) {
                                                delQuota(zk, path, true, true);
            }
        } catch (KeeperException|InterruptedException|IOException ex) {
            throw new CliWrapperException(ex);
        }
        return false;
    }

    
    public static boolean delQuota(ZooKeeper zk, String path,
            boolean bytes, boolean numNodes)
            throws KeeperException, IOException, InterruptedException, MalformedPathException {
        String parentPath = Quotas.quotaZookeeper + path;
        String quotaPath = Quotas.quotaZookeeper + path + "/" + 
                Quotas.limitNode;
        if (zk.exists(quotaPath, false) == null) {
            System.out.println("Quota does not exist for " + path);
            return true;
        }
        byte[] data = null;
        try {
            data = zk.getData(quotaPath, false, new Stat());
        } catch (IllegalArgumentException ex) {
            throw new MalformedPathException(ex.getMessage());
        } catch (KeeperException.NoNodeException ne) {
            System.err.println("quota does not exist for " + path);
            return true;
        }
        StatsTrack strack = new StatsTrack(new String(data));
        if (bytes && !numNodes) {
            strack.setBytes(-1L);
            zk.setData(quotaPath, strack.toString().getBytes(), -1);
        } else if (!bytes && numNodes) {
            strack.setCount(-1);
            zk.setData(quotaPath, strack.toString().getBytes(), -1);
        } else if (bytes && numNodes) {
                                    List<String> children = zk.getChildren(parentPath, false);
                        for (String child : children) {
                zk.delete(parentPath + "/" + child, -1);
            }
                        trimProcQuotas(zk, parentPath);
        }
        return true;
    }

    
    private static boolean trimProcQuotas(ZooKeeper zk, String path)
            throws KeeperException, IOException, InterruptedException {
        if (Quotas.quotaZookeeper.equals(path)) {
            return true;
        }
        List<String> children = zk.getChildren(path, false);
        if (children.size() == 0) {
            zk.delete(path, -1);
            String parent = path.substring(0, path.lastIndexOf('/'));
            return trimProcQuotas(zk, parent);
        } else {
            return true;
        }
    }
}
