package org.apache.zookeeper.server.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.Environment;
import org.apache.zookeeper.Environment.Entry;
import org.apache.zookeeper.Version;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.ReadOnlyZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Commands {
    static final Logger LOG = LoggerFactory.getLogger(Commands.class);

    
    private static Map<String, Command> commands = new HashMap<String, Command>();
    private static Set<String> primaryNames = new HashSet<String>();

    
    public static void registerCommand(Command command) {
        for (String name : command.getNames()) {
            Command prev = commands.put(name, command);
            if (prev != null) {
                LOG.warn("Re-registering command %s (primary name = %s)", name, command.getPrimaryName());
            }
        }
        primaryNames.add(command.getPrimaryName());
    }

    
    public static CommandResponse runCommand(String cmdName, ZooKeeperServer zkServer, Map<String, String> kwargs) {
        if (!commands.containsKey(cmdName)) {
            return new CommandResponse(cmdName, "Unknown command: " + cmdName);
        }
        if (zkServer == null || !zkServer.isRunning()) {
            return new CommandResponse(cmdName, "This ZooKeeper instance is not currently serving requests");
        }
        return commands.get(cmdName).run(zkServer, kwargs);
    }

    
    public static Set<String> getPrimaryNames() {
        return primaryNames;
    }

    
    public static Command getCommand(String cmdName) {
        return commands.get(cmdName);
    }

    static {
        registerCommand(new CnxnStatResetCommand());
        registerCommand(new ConfCommand());
        registerCommand(new ConsCommand());
        registerCommand(new DirsCommand());
        registerCommand(new DumpCommand());
        registerCommand(new EnvCommand());
        registerCommand(new GetTraceMaskCommand());
        registerCommand(new IsroCommand());
        registerCommand(new MonitorCommand());
        registerCommand(new RuokCommand());
        registerCommand(new SetTraceMaskCommand());
        registerCommand(new SrvrCommand());
        registerCommand(new StatCommand());
        registerCommand(new StatResetCommand());
        registerCommand(new WatchCommand());
        registerCommand(new WatchesByPathCommand());
        registerCommand(new WatchSummaryCommand());
    }

    
    public static class CnxnStatResetCommand extends CommandBase {
        public CnxnStatResetCommand() {
            super(Arrays.asList("connection_stat_reset", "crst"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.getServerCnxnFactory().resetAllConnectionStats();
            return response;

        }
    }

    
    public static class ConfCommand extends CommandBase {
        public ConfCommand() {
            super(Arrays.asList("configuration", "conf", "config"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.putAll(zkServer.getConf().toMap());
            return response;
        }
    }

    
    public static class ConsCommand extends CommandBase {
        public ConsCommand() {
            super(Arrays.asList("connections", "cons"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            ServerCnxnFactory serverCnxnFactory = zkServer.getServerCnxnFactory();
            if (serverCnxnFactory != null) {
                response.put("connections", serverCnxnFactory.getAllConnectionInfo(false));
            } else {
                response.put("connections", Collections.emptyList());
            }
            ServerCnxnFactory secureServerCnxnFactory = zkServer.getSecureServerCnxnFactory();
            if (secureServerCnxnFactory != null) {
                response.put("secure_connections", secureServerCnxnFactory.getAllConnectionInfo(false));
            } else {
                response.put("secure_connections", Collections.emptyList());
            }
            return response;
        }
    }

    
    public static class DirsCommand extends CommandBase {
        public DirsCommand() {
            super(Arrays.asList("dirs"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("datadir_size", zkServer.getDataDirSize());
            response.put("logdir_size", zkServer.getLogDirSize());
            return response;
        }
    }

    
    public static class DumpCommand extends CommandBase {
        public DumpCommand() {
            super(Arrays.asList("dump"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("expiry_time_to_session_ids", zkServer.getSessionExpiryMap());
            response.put("session_id_to_ephemeral_paths", zkServer.getEphemerals());
            return response;
        }
    }

    
    public static class EnvCommand extends CommandBase {
        public EnvCommand() {
            super(Arrays.asList("environment", "env", "envi"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            for (Entry e : Environment.list()) {
                response.put(e.getKey(), e.getValue());
            }
            return response;
        }
    }

    
    public static class GetTraceMaskCommand extends CommandBase {
        public GetTraceMaskCommand() {
            super(Arrays.asList("get_trace_mask", "gtmk"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("tracemask", ZooTrace.getTextTraceLevel());
            return response;
        }
    }

    
    public static class IsroCommand extends CommandBase {
        public IsroCommand() {
            super(Arrays.asList("is_read_only", "isro"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            return response;
        }
    }

    
    public static class MonitorCommand extends CommandBase {
        public MonitorCommand() {
            super(Arrays.asList("monitor", "mntr"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            ZKDatabase zkdb = zkServer.getZKDatabase();
            ServerStats stats = zkServer.serverStats();

            CommandResponse response = initializeResponse();

            response.put("version", Version.getFullVersion());

            response.put("avg_latency", stats.getAvgLatency());
            response.put("max_latency", stats.getMaxLatency());
            response.put("min_latency", stats.getMinLatency());

            response.put("packets_received", stats.getPacketsReceived());
            response.put("packets_sent", stats.getPacketsSent());
            response.put("num_alive_connections", stats.getNumAliveClientConnections());

            response.put("outstanding_requests", stats.getOutstandingRequests());

            response.put("server_state", stats.getServerState());
            response.put("znode_count", zkdb.getNodeCount());

            response.put("watch_count", zkdb.getDataTree().getWatchCount());
            response.put("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
            response.put("approximate_data_size", zkdb.getDataTree().approximateDataSize());

            OSMXBean osMbean = new OSMXBean();
            response.put("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
            response.put("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());

            response.put("last_client_response_size", stats.getClientResponseStats().getLastBufferSize());
            response.put("max_client_response_size", stats.getClientResponseStats().getMaxBufferSize());
            response.put("min_client_response_size", stats.getClientResponseStats().getMinBufferSize());

            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer) zkServer).getLeader();

                response.put("followers", leader.getLearners().size());
                response.put("synced_followers", leader.getForwardingFollowers().size());
                response.put("pending_syncs", leader.getNumPendingSyncs());

                response.put("last_proposal_size", leader.getProposalStats().getLastBufferSize());
                response.put("max_proposal_size", leader.getProposalStats().getMaxBufferSize());
                response.put("min_proposal_size", leader.getProposalStats().getMinBufferSize());
            }

            return response;

        }}

    
    public static class RuokCommand extends CommandBase {
        public RuokCommand() {
            super(Arrays.asList("ruok"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            return initializeResponse();
        }
    }

    
    public static class SetTraceMaskCommand extends CommandBase {
        public SetTraceMaskCommand() {
            super(Arrays.asList("set_trace_mask", "stmk"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            long traceMask;
            if (!kwargs.containsKey("traceMask")) {
                response.put("error", "setTraceMask requires long traceMask argument");
                return response;
            }
            try {
                traceMask = Long.parseLong(kwargs.get("traceMask"));
            } catch (NumberFormatException e) {
                response.put("error", "setTraceMask requires long traceMask argument, got "
                                      + kwargs.get("traceMask"));
                return response;
            }

            ZooTrace.setTextTraceLevel(traceMask);
            response.put("tracemask", traceMask);
            return response;
        }
    }

    
    public static class SrvrCommand extends CommandBase {
        public SrvrCommand() {
            super(Arrays.asList("server_stats", "srvr"));
        }

                protected SrvrCommand(List<String> names) {
            super(names);
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            LOG.info("running stat");
            response.put("version", Version.getFullVersion());
            response.put("read_only", zkServer instanceof ReadOnlyZooKeeperServer);
            response.put("server_stats", zkServer.serverStats());
            response.put("client_response", zkServer.serverStats().getClientResponseStats());
            if (zkServer instanceof LeaderZooKeeperServer) {
                Leader leader = ((LeaderZooKeeperServer)zkServer).getLeader();
                response.put("proposal_stats", leader.getProposalStats());
            }
            response.put("node_count", zkServer.getZKDatabase().getNodeCount());
            return response;
        }
    }

    
    public static class StatCommand extends SrvrCommand {
        public StatCommand() {
            super(Arrays.asList("stats", "stat"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = super.run(zkServer, kwargs);

            final Iterable<Map<String, Object>> connections;
            if (zkServer.getServerCnxnFactory() != null) {
                connections = zkServer.getServerCnxnFactory().getAllConnectionInfo(true);
            } else {
                connections = Collections.emptyList();
            }
            response.put("connections", connections);

            final Iterable<Map<String, Object>> secureConnections;
            if (zkServer.getSecureServerCnxnFactory() != null) {
                secureConnections = zkServer.getSecureServerCnxnFactory().getAllConnectionInfo(true);
            } else {
                secureConnections = Collections.emptyList();
            }
            response.put("secure_connections", secureConnections);
            return response;
        }
    }

    
    public static class StatResetCommand extends CommandBase {
        public StatResetCommand() {
            super(Arrays.asList("stat_reset", "srst"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            CommandResponse response = initializeResponse();
            zkServer.serverStats().reset();
            return response;
        }
    }

    
    public static class WatchCommand extends CommandBase {
        public WatchCommand() {
            super(Arrays.asList("watches", "wchc"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("session_id_to_watched_paths", dt.getWatches().toMap());
            return response;
        }
    }

    
    public static class WatchesByPathCommand extends CommandBase {
        public WatchesByPathCommand() {
            super(Arrays.asList("watches_by_path", "wchp"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.put("path_to_session_ids", dt.getWatchesByPath().toMap());
            return response;
        }
    }

    
    public static class WatchSummaryCommand extends CommandBase {
        public WatchSummaryCommand() {
            super(Arrays.asList("watch_summary", "wchs"));
        }

        @Override
        public CommandResponse run(ZooKeeperServer zkServer, Map<String, String> kwargs) {
            DataTree dt = zkServer.getZKDatabase().getDataTree();
            CommandResponse response = initializeResponse();
            response.putAll(dt.getWatchesSummary().toMap());
            return response;
        }
    }

    private Commands() {}
}
