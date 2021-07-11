package org.apache.zookeeper.server.util;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

public class ConfigUtilsTest {

    @Test
    public void testSplitServerConfig() throws ConfigException {
        String[] nsa = ConfigUtils.getHostAndPort("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443");
        System.out.println(nsa[0]);
        assertEquals(nsa[0], "2001:db8:85a3:8d3:1319:8a2e:370:7348");
        assertEquals(nsa[1], "443");
    }

    @Test
    public void testSplitServerConfig2() throws ConfigException {
        String[] nsa = ConfigUtils.getHostAndPort("127.0.0.1:443");
        assertEquals(nsa.length, 2, 0);
    }

    @Test(expected = ConfigException.class)
    public void testSplitServerConfig3() throws ConfigException {
        String[] nsa = ConfigUtils.getHostAndPort("[2001:db8:85a3:8d3:1319:8a2e:370:7348");
    }

    @Test
    public void testSplitServerConfig4() throws ConfigException {
        String[] nsa = ConfigUtils.getHostAndPort("2001:db8:85a3:8d3:1319:8a2e:370:7348:443");
        assertFalse(nsa.length == 2);
    }
}
