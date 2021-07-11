package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientCanonicalizeTest extends ZKTestCase {
    @Test
    public void testClientCanonicalization() throws IOException, InterruptedException {
        SaslServerPrincipal.WrapperInetSocketAddress addr = mock(SaslServerPrincipal.WrapperInetSocketAddress.class);
        SaslServerPrincipal.WrapperInetAddress ia = mock(SaslServerPrincipal.WrapperInetAddress.class);

        when(addr.getHostName()).thenReturn("zookeeper.apache.org");
        when(addr.getAddress()).thenReturn(ia);
        when(ia.getCanonicalHostName()).thenReturn("zk1.apache.org");
        when(ia.getHostAddress()).thenReturn("127.0.0.1");

        ZKClientConfig conf = new ZKClientConfig();
        String principal = SaslServerPrincipal.getServerPrincipal(addr, conf);
        Assert.assertEquals("The computed principal does not appear to have been canonicalized", "zookeeper/zk1.apache.org", principal);
    }

    @Test
    public void testClientNoCanonicalization() throws IOException, InterruptedException {
        SaslServerPrincipal.WrapperInetSocketAddress addr = mock(SaslServerPrincipal.WrapperInetSocketAddress.class);
        SaslServerPrincipal.WrapperInetAddress ia = mock(SaslServerPrincipal.WrapperInetAddress.class);

        when(addr.getHostName()).thenReturn("zookeeper.apache.org");
        when(addr.getAddress()).thenReturn(ia);
        when(ia.getCanonicalHostName()).thenReturn("zk1.apache.org");
        when(ia.getHostAddress()).thenReturn("127.0.0.1");

        ZKClientConfig conf = new ZKClientConfig();
        conf.setProperty(ZKClientConfig.ZK_SASL_CLIENT_CANONICALIZE_HOSTNAME, "false");
        String principal = SaslServerPrincipal.getServerPrincipal(addr, conf);
        Assert.assertEquals("The computed principal does appears to have been canonicalized incorrectly", "zookeeper/zookeeper.apache.org",
            principal);
    }

    @Test
    public void testClientCanonicalizationToIp() throws IOException, InterruptedException {
        SaslServerPrincipal.WrapperInetSocketAddress addr = mock(SaslServerPrincipal.WrapperInetSocketAddress.class);
        SaslServerPrincipal.WrapperInetAddress ia = mock(SaslServerPrincipal.WrapperInetAddress.class);

        when(addr.getHostName()).thenReturn("zookeeper.apache.org");
        when(addr.getAddress()).thenReturn(ia);
        when(ia.getCanonicalHostName()).thenReturn("127.0.0.1");
        when(ia.getHostAddress()).thenReturn("127.0.0.1");

        ZKClientConfig conf = new ZKClientConfig();
        String principal = SaslServerPrincipal.getServerPrincipal(addr, conf);
        Assert.assertEquals("The computed principal does appear to have falled back to the original host name",
            "zookeeper/zookeeper.apache.org", principal);
    }

    @Test
    public void testGetServerPrincipalReturnConfiguredPrincipalName() {
        ZKClientConfig config = new ZKClientConfig();
        String configuredPrincipal = "zookeeper/zookeeper.apache.org@APACHE.ORG";
        config.setProperty(ZKClientConfig.ZOOKEEPER_SERVER_PRINCIPAL, configuredPrincipal);

                String serverPrincipal = SaslServerPrincipal.getServerPrincipal((InetSocketAddress) null, config);
        Assert.assertEquals(configuredPrincipal, serverPrincipal);
    }

}
