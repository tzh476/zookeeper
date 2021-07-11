package org.apache.zookeeper.test;

import org.junit.runners.Suite;


@Suite.SuiteClasses({
        ACLTest.class,
        AsyncOpsTest.class,
        ChrootClientTest.class,
        ClientTest.class,
        FourLetterWordsTest.class,
        NullDataTest.class,
        ReconfigTest.class,
        SessionTest.class,
        WatcherTest.class
})
public class NettyNettySuiteTest extends NettyNettySuiteBase {
}
