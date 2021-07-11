package org.apache.zookeeper.server.command;

import java.io.PrintWriter;

import org.apache.zookeeper.Version;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.util.OSMXBean;

public class MonitorCommand extends AbstractFourLetterCommand {

    MonitorCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        super(pw, serverCnxn);
    }

    @Override
    public void commandRun() {
        if (!isZKServerRunning()) {
            pw.println(ZK_NOT_SERVING);
            return;
        }
        ZKDatabase zkdb = zkServer.getZKDatabase();
        ServerStats stats = zkServer.serverStats();

        print("version", Version.getFullVersion());

        print("avg_latency", stats.getAvgLatency());
        print("max_latency", stats.getMaxLatency());
        print("min_latency", stats.getMinLatency());

        print("packets_received", stats.getPacketsReceived());
        print("packets_sent", stats.getPacketsSent());
        print("num_alive_connections", stats.getNumAliveClientConnections());

        print("outstanding_requests", stats.getOutstandingRequests());

        print("server_state", stats.getServerState());
        print("znode_count", zkdb.getNodeCount());

        print("watch_count", zkdb.getDataTree().getWatchCount());
        print("ephemerals_count", zkdb.getDataTree().getEphemeralsCount());
        print("approximate_data_size", zkdb.getDataTree().approximateDataSize());

        OSMXBean osMbean = new OSMXBean();
        if (osMbean != null && osMbean.getUnix() == true) {
            print("open_file_descriptor_count", osMbean.getOpenFileDescriptorCount());
            print("max_file_descriptor_count", osMbean.getMaxFileDescriptorCount());
        }

        if (stats.getServerState().equals("leader")) {
            Leader leader = ((LeaderZooKeeperServer)zkServer).getLeader();

            print("followers", leader.getLearners().size());
            print("synced_followers", leader.getForwardingFollowers().size());
            print("pending_syncs", leader.getNumPendingSyncs());

            print("last_proposal_size", leader.getProposalStats().getLastBufferSize());
            print("max_proposal_size", leader.getProposalStats().getMaxBufferSize());
            print("min_proposal_size", leader.getProposalStats().getMinBufferSize());
        }
    }

    private void print(String key, long number) {
        print(key, "" + number);
    }

    private void print(String key, String value) {
        pw.print("zk_");
        pw.print(key);
        pw.print("\t");
        pw.println(value);
    }

}
