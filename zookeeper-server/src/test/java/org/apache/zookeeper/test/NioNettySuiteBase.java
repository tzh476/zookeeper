package org.apache.zookeeper.test;

import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
public class NioNettySuiteBase {
    @BeforeClass
    public static void setUp() {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY,
                NettyServerCnxnFactory.class.getName());
        System.setProperty("zookeeper.admin.enableServer", "false");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
    }

    @Before
    public void setUpTest() throws Exception {
        TestByteBufAllocatorTestHelper.setTestAllocator(TestByteBufAllocator.getInstance());
    }

    @After
    public void tearDownTest() throws Exception {
        TestByteBufAllocatorTestHelper.clearTestAllocator();
        TestByteBufAllocator.checkForLeaks();
    }
}
