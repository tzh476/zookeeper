package org.apache.zookeeper.test;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.client.ConnectStringParser;
import org.junit.Assert;
import org.junit.Test;

public class ConnectStringParserTest extends ZKTestCase{

    @Test
    public void testSingleServerChrootPath(){
        String chrootPath = "/hallo/welt";
        String servers = "10.10.10.1";
        assertChrootPath(chrootPath,
                new ConnectStringParser(servers+chrootPath));
    }

    @Test
    public void testMultipleServersChrootPath(){
        String chrootPath = "/hallo/welt";
        String servers = "10.10.10.1,10.10.10.2";
        assertChrootPath(chrootPath,
                new ConnectStringParser(servers+chrootPath));
    }

    @Test
    public void testParseServersWithoutPort(){
        String servers = "10.10.10.1,10.10.10.2";
        ConnectStringParser parser = new ConnectStringParser(servers);

        Assert.assertEquals("10.10.10.1", parser.getServerAddresses().get(0).getHostString());
        Assert.assertEquals("10.10.10.2", parser.getServerAddresses().get(1).getHostString());
    }

    @Test
    public void testParseServersWithPort(){
        String servers = "10.10.10.1:112,10.10.10.2:110";
        ConnectStringParser parser = new ConnectStringParser(servers);

        Assert.assertEquals("10.10.10.1", parser.getServerAddresses().get(0).getHostString());
        Assert.assertEquals("10.10.10.2", parser.getServerAddresses().get(1).getHostString());

        Assert.assertEquals(112, parser.getServerAddresses().get(0).getPort());
        Assert.assertEquals(110, parser.getServerAddresses().get(1).getPort());
    }

    private void assertChrootPath(String expected, ConnectStringParser parser){
        Assert.assertEquals(expected, parser.getChrootPath());
    }
}
