package org.apache.zookeeper.common;

import org.apache.zookeeper.server.ZooKeeperThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Consumer;


public final class FileChangeWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(FileChangeWatcher.class);

    public enum State {
        NEW,              STARTING,         RUNNING,          STOPPING,         STOPPED       }

    private final WatcherThread watcherThread;
    private State state; 
    
    public FileChangeWatcher(Path dirPath, Consumer<WatchEvent<?>> callback) throws IOException {
        FileSystem fs = dirPath.getFileSystem();
        WatchService watchService = fs.newWatchService();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Registering with watch service: " + dirPath);
        }
        dirPath.register(
                watchService,
                new WatchEvent.Kind<?>[]{
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.OVERFLOW});
        state = State.NEW;
        this.watcherThread = new WatcherThread(watchService, callback);
        this.watcherThread.setDaemon(true);
    }

    
    public synchronized State getState() {
        return state;
    }

    
    synchronized void waitForState(State desiredState) throws InterruptedException {
        while (this.state != desiredState) {
            this.wait();
        }
    }

    
    private synchronized void setState(State newState) {
        state = newState;
        this.notifyAll();
    }

    
    private synchronized boolean compareAndSetState(State expected, State update) {
        if (state == expected) {
            setState(update);
            return true;
        } else {
            return false;
        }
    }

    
    private synchronized boolean compareAndSetState(State[] expectedStates, State update) {
        for (State expected : expectedStates) {
            if (state == expected) {
                setState(update);
                return true;
            }
        }
        return false;
    }

    
    public void start() {
        if (!compareAndSetState(State.NEW, State.STARTING)) {
                        return;
        }
        this.watcherThread.start();
    }

    
    public void stop() {
        if (compareAndSetState(
                new State[]{State.RUNNING, State.STARTING},
                State.STOPPING)) {
            watcherThread.interrupt();
        }
    }

    
    private class WatcherThread extends ZooKeeperThread {
        private static final String THREAD_NAME = "FileChangeWatcher";

        final WatchService watchService;
        final Consumer<WatchEvent<?>> callback;

        WatcherThread(WatchService watchService, Consumer<WatchEvent<?>> callback) {
            super(THREAD_NAME);
            this.watchService = watchService;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                LOG.info(getName() + " thread started");
                if (!compareAndSetState(
                        FileChangeWatcher.State.STARTING,
                        FileChangeWatcher.State.RUNNING)) {
                                                            FileChangeWatcher.State state = FileChangeWatcher.this.getState();
                    if (state != FileChangeWatcher.State.STOPPING) {
                        throw new IllegalStateException("Unexpected state: " + state);
                    }
                    return;
                }
                runLoop();
            } catch (Exception e) {
                LOG.warn("Error in runLoop()", e);
                throw e;
            } finally {
                try {
                    watchService.close();
                } catch (IOException e) {
                    LOG.warn("Error closing watch service", e);
                }
                LOG.info(getName() + " thread finished");
                FileChangeWatcher.this.setState(FileChangeWatcher.State.STOPPED);
            }
        }

        private void runLoop() {
            while (FileChangeWatcher.this.getState() == FileChangeWatcher.State.RUNNING) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException|ClosedWatchServiceException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getName() + " was interrupted and is shutting down ...");
                    }
                    break;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Got file changed event: " + event.kind() + " with context: " + event.context());
                    }
                    try {
                        callback.accept(event);
                    } catch (Throwable e) {
                        LOG.error("Error from callback", e);
                    }
                }
                boolean isKeyValid = key.reset();
                if (!isKeyValid) {
                                                                                                    LOG.error("Watch key no longer valid, maybe the directory is inaccessible?");
                    break;
                }
            }
        }
    }
}
