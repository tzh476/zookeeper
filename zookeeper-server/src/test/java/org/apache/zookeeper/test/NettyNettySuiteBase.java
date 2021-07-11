package org.apache.zookeeper.test;

import org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
public class NettyNettySuiteBase {
    @BeforeClass
    public static void setUp() {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY,
                NettyServerCnxnFactory.class.getName());
        System.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET,
                ClientCnxnSocketNetty.class.getName());
        System.setProperty("zookeeper.admin.enableServer", "false");
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
        System.clearProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
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
