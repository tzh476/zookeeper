package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public class ContainerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
    private final ZKDatabase zkDb;
    private final RequestProcessor requestProcessor;
    private final int checkIntervalMs;
    private final int maxPerMinute;
    private final Timer timer;
    private final AtomicReference<TimerTask> task = new AtomicReference<TimerTask>(null);

    
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor,
                            int checkIntervalMs, int maxPerMinute) {
        this.zkDb = zkDb;
        this.requestProcessor = requestProcessor;
        this.checkIntervalMs = checkIntervalMs;
        this.maxPerMinute = maxPerMinute;
        timer = new Timer("ContainerManagerTask", true);

        LOG.info(String.format("Using checkIntervalMs=%d maxPerMinute=%d",
                checkIntervalMs, maxPerMinute));
    }

    
    public void start() {
        if (task.get() == null) {
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        checkContainers();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("interrupted");
                        cancel();
                    } catch ( Throwable e ) {
                        LOG.error("Error checking containers", e);
                    }
                }
            };
            if (task.compareAndSet(null, timerTask)) {
                timer.scheduleAtFixedRate(timerTask, checkIntervalMs,
                        checkIntervalMs);
            }
        }
    }

    
    public void stop() {
        TimerTask timerTask = task.getAndSet(null);
        if (timerTask != null) {
            timerTask.cancel();
        }
        timer.cancel();
    }

    
    public void checkContainers()
            throws InterruptedException {
        long minIntervalMs = getMinIntervalMs();
        for (String containerPath : getCandidates()) {
            long startMs = Time.currentElapsedTime();

            ByteBuffer path = ByteBuffer.wrap(containerPath.getBytes());
            Request request = new Request(null, 0, 0,
                    ZooDefs.OpCode.deleteContainer, path, null);
            try {
                LOG.info("Attempting to delete candidate container: {}",
                        containerPath);
                requestProcessor.processRequest(request);
            } catch (Exception e) {
                LOG.error("Could not delete container: {}",
                        containerPath, e);
            }

            long elapsedMs = Time.currentElapsedTime() - startMs;
            long waitMs = minIntervalMs - elapsedMs;
            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }
        }
    }

        protected long getMinIntervalMs() {
        return TimeUnit.MINUTES.toMillis(1) / maxPerMinute;
    }

        protected Collection<String> getCandidates() {
        Set<String> candidates = new HashSet<String>();
        for (String containerPath : zkDb.getDataTree().getContainers()) {
            DataNode node = zkDb.getDataTree().getNode(containerPath);
            
            if ((node != null) && (node.stat.getCversion() > 0) &&
                    (node.getChildren().size() == 0)) {
                candidates.add(containerPath);
            }
        }
        for (String ttlPath : zkDb.getDataTree().getTtls()) {
            DataNode node = zkDb.getDataTree().getNode(ttlPath);
            if (node != null) {
                Set<String> children = node.getChildren();
                if ((children == null) || (children.size() == 0)) {
                    if ( EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL ) {
                        long elapsed = getElapsed(node);
                        long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
                        if ((ttl != 0) && (getElapsed(node) > ttl)) {
                            candidates.add(ttlPath);
                        }
                    }
                }
            }
        }
        return candidates;
    }

        protected long getElapsed(DataNode node) {
        return Time.currentWallTime() - node.stat.getMtime();
    }
}
