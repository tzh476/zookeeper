package org.apache.zookeeper.server;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.proto.SetDataRequest;
import org.junit.Assert;
import org.junit.Test;


public class ToStringTest extends ZKTestCase {
    
    @Test
    public void testJuteToString() {
        SetDataRequest req = new SetDataRequest(null, null, 0);
        Assert.assertNotSame("ERROR", req.toString());
    }
}
