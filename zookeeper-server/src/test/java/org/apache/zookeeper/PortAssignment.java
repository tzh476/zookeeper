package org.apache.zookeeper;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PortAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(PortAssignment.class);

                                        private static final int GLOBAL_BASE_PORT = 11221;
    private static final int GLOBAL_MAX_PORT = 32767;

    private static PortRange portRange = null;
    private static int nextPort;

    
    public synchronized static int unique() {
        if (portRange == null) {
            Integer threadId = Integer.getInteger("zookeeper.junit.threadid");
            portRange = setupPortRange(System.getProperty("test.junit.threads"),
                    threadId != null ? "threadid=" + threadId :
                    System.getProperty("sun.java.command"));
            nextPort = portRange.getMinimum();
        }
        int candidatePort = nextPort;
        for (;;) {
            ++candidatePort;
            if (candidatePort > portRange.getMaximum()) {
                candidatePort = portRange.getMinimum();
            }
            if (candidatePort == nextPort) {
                throw new IllegalStateException(String.format(
                        "Could not assign port from range %s.  The entire " +
                        "range has been exhausted.", portRange));
            }
            try {
                ServerSocket s = new ServerSocket(candidatePort);
                s.close();
                nextPort = candidatePort;
                LOG.info("Assigned port {} from range {}.", nextPort, portRange);
                return nextPort;
            } catch (IOException e) {
                LOG.debug("Could not bind to port {} from range {}.  " +
                        "Attempting next port.", candidatePort, portRange, e);
            }
        }
    }

    
    static PortRange setupPortRange(String strProcessCount, String cmdLine) {
        Integer processCount = null;
        if (strProcessCount != null && !strProcessCount.isEmpty()) {
            try {
                processCount = Integer.valueOf(strProcessCount);
            } catch (NumberFormatException e) {
                LOG.warn("Error parsing test.junit.threads = {}.",
                         strProcessCount, e);
            }
        }

        Integer threadId = null;
        if (processCount != null) {
            if (cmdLine != null && !cmdLine.isEmpty()) {
                Matcher m = Pattern.compile("threadid=(\\d+)").matcher(cmdLine);
                if (m.find()) {
                    try {
                        threadId = Integer.valueOf(m.group(1));
                    } catch (NumberFormatException e) {
                        LOG.warn("Error parsing threadid from {}.", cmdLine, e);
                    }
                }
            }
        }

        final PortRange newPortRange;
        if (processCount != null && processCount > 1 && threadId != null) {
                                                            int portRangeSize = (GLOBAL_MAX_PORT - GLOBAL_BASE_PORT) /
                    processCount;
            int minPort = GLOBAL_BASE_PORT + ((threadId - 1) * portRangeSize);
            int maxPort = minPort + portRangeSize - 1;
            newPortRange = new PortRange(minPort, maxPort);
            LOG.info("Test process {}/{} using ports from {}.", threadId,
                    processCount, newPortRange);
        } else {
                                    newPortRange = new PortRange(GLOBAL_BASE_PORT, GLOBAL_MAX_PORT);
            LOG.info("Single test process using ports from {}.", newPortRange);
        }

        return newPortRange;
    }

    
    static final class PortRange {
        private final int minimum;
        private final int maximum;

        
        PortRange(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        
        int getMaximum() {
            return maximum;
        }

        
        int getMinimum() {
            return minimum;
        }

        @Override
        public String toString() {
            return String.format("%d - %d", minimum, maximum);
        }
    }

    
    private PortAssignment() {
    }
}
