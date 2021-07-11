package org.apache.zookeeper.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.common.X509Exception.SSLContextException;

import static org.apache.zookeeper.client.FourLetterWordMain.send4LetterWord;

import org.junit.Assert;
import org.junit.Test;

public class FourLetterWordsQuorumTest extends QuorumBase {
    protected static final Logger LOG =
        LoggerFactory.getLogger(FourLetterWordsQuorumTest.class);

    
    @Test
    public void testFourLetterWords() throws Exception {
        String servers[] = hostPort.split(",");
        for (String hp : servers) {
            verify(hp, "ruok", "imok");
            verify(hp, "envi", "java.version");
            verify(hp, "conf", "clientPort");
            verify(hp, "stat", "Outstanding");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", "queued");
            verify(hp, "dump", "Session");
            verify(hp, "wchs", "watches");
            verify(hp, "wchp", "");
            verify(hp, "wchc", "");

            verify(hp, "srst", "reset");
            verify(hp, "crst", "reset");

            verify(hp, "stat", "Outstanding");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", "queued");

            TestableZooKeeper zk = createClient(hp);
            String sid = getHexSessionId(zk.getSessionId());

            verify(hp, "stat", "queued");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", sid);
            verify(hp, "dump", sid);
            verify(hp, "dirs", "size");

            zk.getData("/", true, null);

            verify(hp, "stat", "queued");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", sid);
            verify(hp, "dump", sid);
            verify(hp, "wchs", "watching 1");
            verify(hp, "wchp", sid);
            verify(hp, "wchc", sid);
            verify(hp, "dirs", "size");

            zk.close();

            verify(hp, "ruok", "imok");
            verify(hp, "envi", "java.version");
            verify(hp, "conf", "clientPort");
            verify(hp, "stat", "Outstanding");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", "queued");
            verify(hp, "dump", "Session");
            verify(hp, "wchs", "watch");
            verify(hp, "wchp", "");
            verify(hp, "wchc", "");
            verify(hp, "dirs", "size");

            verify(hp, "srst", "reset");
            verify(hp, "crst", "reset");

            verify(hp, "stat", "Outstanding");
            verify(hp, "srvr", "Outstanding");
            verify(hp, "cons", "queued");

            verify(hp, "mntr", "zk_version\t");
        }
    }

    private void verify(String hp, String cmd, String expected)
        throws IOException, SSLContextException
    {
        for(HostPort hpobj: parseHostPortList(hp)) {
            String resp = send4LetterWord(hpobj.host, hpobj.port, cmd);
            LOG.info("cmd " + cmd + " expected " + expected + " got " + resp);
            if (cmd.equals("dump")) {
                Assert.assertTrue(resp.contains(expected)
                        || resp.contains("Sessions with Ephemerals"));
            } else {
                Assert.assertTrue(resp.contains(expected));
            }
        }
    }
}
