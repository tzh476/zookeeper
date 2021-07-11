package org.apache.zookeeper.server.command;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractFourLetterCommand {
    private static final Logger LOG = LoggerFactory
        .getLogger(AbstractFourLetterCommand.class);

    public static final String ZK_NOT_SERVING = "This ZooKeeper instance is not currently serving requests";

    protected PrintWriter pw;
    protected ServerCnxn serverCnxn;
    protected ZooKeeperServer zkServer;
    protected ServerCnxnFactory factory;

    public AbstractFourLetterCommand(PrintWriter pw, ServerCnxn serverCnxn) {
        this.pw = pw;
        this.serverCnxn = serverCnxn;
    }

    public void start() {
        run();
    }

    public void run() {
        try {
            commandRun();
        } catch (IOException ie) {
            LOG.error("Error in running command ", ie);
        } finally {
            serverCnxn.cleanupWriterSocket(pw);
        }
    }

    public void setZkServer(ZooKeeperServer zkServer) {
        this.zkServer = zkServer;
    }

    
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    public void setFactory(ServerCnxnFactory factory) {
        this.factory = factory;
    }

    public abstract void commandRun() throws IOException;
}
