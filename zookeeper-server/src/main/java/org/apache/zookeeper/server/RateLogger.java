package org.apache.zookeeper.server;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;

public class RateLogger {
    public RateLogger(Logger log) {
        LOG = log;
    }

    private final Logger LOG;
    private String msg = null;
    private long timestamp;
    private int count = 0;

    public void flush() {
        if (msg != null) {
            if (count > 1) {
                LOG.warn("[" + count + " times] " + msg);
            } else if (count == 1) {
                LOG.warn(msg);
            }
        }
        msg = null;
        count = 0;
    }

    public void rateLimitLog(String newMsg) {
        long now = Time.currentElapsedTime();
        if (newMsg.equals(msg)) {
            ++count;
            if (now - timestamp >= 100) {
                flush();
                msg = newMsg;
                timestamp = now;
            }
        } else {
            flush();
            msg = newMsg;
            timestamp = now;
            LOG.warn(msg);
        }
    }
}
