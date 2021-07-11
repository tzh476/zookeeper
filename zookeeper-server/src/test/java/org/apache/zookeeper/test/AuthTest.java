package org.apache.zookeeper.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

public class AuthTest extends ClientBase {
    static {
                System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "super:D/InIHSb7yEEbrWz8b9l71RjZJU=");    
        System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.test.InvalidAuthProvider");
    }

    private final CountDownLatch authFailed = new CountDownLatch(1);

    @Override
    protected TestableZooKeeper createClient(String hp)
    throws IOException, InterruptedException
    {
        MyWatcher watcher = new MyWatcher();
        return createClient(watcher, hp);
    }

    private class MyWatcher extends CountdownWatcher {
        @Override
        public synchronized void process(WatchedEvent event) {
            if (event.getState() == KeeperState.AuthFailed) {
                authFailed.countDown();
            }
            else {
                super.process(event);
            }
        }
    }

    @Test
    public void testBadAuthNotifiesWatch() throws Exception {
        ZooKeeper zk = createClient();
        try {
            zk.addAuthInfo("FOO", "BAR".getBytes());
            zk.getData("/path1", false, null);
            Assert.fail("Should get auth state error");
        } catch(KeeperException.AuthFailedException e) {
            if(!authFailed.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS))
            {
                Assert.fail("Should have called my watcher");
            }
        }
        finally {
            zk.close();
        }
    }
    
    @Test
    public void testBadAuthThenSendOtherCommands() throws Exception {
        ZooKeeper zk = createClient();     
        try {        
            zk.addAuthInfo("INVALID", "BAR".getBytes());
            zk.exists("/foobar", false);             
            zk.getData("/path1", false, null);
            Assert.fail("Should get auth state error");
        } catch(KeeperException.AuthFailedException e) {
            if(!authFailed.await(CONNECTION_TIMEOUT,
                    TimeUnit.MILLISECONDS))
            {
                Assert.fail("Should have called my watcher");
            }
        }
        finally {
            zk.close();          
        }
    }

    
    @Test
    public void testSuper() throws Exception {
        ZooKeeper zk = createClient();
        try {
            zk.addAuthInfo("digest", "pat:pass".getBytes());
            zk.create("/path1", null, Ids.CREATOR_ALL_ACL,
                    CreateMode.PERSISTENT);
            zk.close();
                        zk = createClient();
            try {
                zk.getData("/path1", false, null);
                Assert.fail("auth verification");
            } catch (KeeperException.NoAuthException e) {
                            }
            zk.close();
                        zk = createClient();
            zk.addAuthInfo("digest", "pat:pass2".getBytes());
            try {
                zk.getData("/path1", false, null);
                Assert.fail("auth verification");
            } catch (KeeperException.NoAuthException e) {
                            }
            zk.close();
                        zk = createClient();
            zk.addAuthInfo("digest", "super:test2".getBytes());
            try {
                zk.getData("/path1", false, null);
                Assert.fail("auth verification");
            } catch (KeeperException.NoAuthException e) {
                            }
            zk.close();
                        zk = createClient();
            zk.addAuthInfo("digest", "super:test".getBytes());
            zk.getData("/path1", false, null);
        } finally {
            zk.close();
        }
    }
    
    @Test
    public void testSuperACL() throws Exception {
    	 ZooKeeper zk = createClient();
         try {
             zk.addAuthInfo("digest", "pat:pass".getBytes());
             zk.create("/path1", null, Ids.CREATOR_ALL_ACL,
                     CreateMode.PERSISTENT);
             zk.close();
                          zk = createClient();
             zk.addAuthInfo("digest", "super:test".getBytes());
             zk.getData("/path1", false, null);
             
             zk.setACL("/path1", Ids.READ_ACL_UNSAFE, -1);
             zk.create("/path1/foo", null, Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
           
             
             zk.setACL("/path1", Ids.OPEN_ACL_UNSAFE, -1);
        	 
         } finally {
             zk.close();
         }
    }
}
