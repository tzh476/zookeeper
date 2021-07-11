package org.apache.zookeeper.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ClientSkipACLTest extends ClientTest {
    @BeforeClass
    static public void setup() {
        System.setProperty("zookeeper.skipACL", "yes");
    }

    @AfterClass
    static public void teardown() {
        System.clearProperty("zookeeper.skipACL");
    }
}