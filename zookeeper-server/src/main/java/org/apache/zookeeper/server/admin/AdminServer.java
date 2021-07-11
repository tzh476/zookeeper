package org.apache.zookeeper.server.admin;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.ZooKeeperServer;


@InterfaceAudience.Public
public interface AdminServer {
    public void start() throws AdminServerException;
    public void shutdown() throws AdminServerException;
    public void setZooKeeperServer(ZooKeeperServer zkServer);

    @InterfaceAudience.Public
    public class AdminServerException extends Exception {
        private static final long serialVersionUID = 1L;
        public AdminServerException(String message, Throwable cause) {
            super(message, cause);
        }
        public AdminServerException(Throwable cause) {
            super(cause);
        }
    }
}