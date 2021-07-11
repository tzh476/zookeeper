package org.apache.zookeeper.test;

import java.util.EnumSet;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Assert;
import org.junit.Test;

public class WatchedEventTest extends ZKTestCase {

    @Test
    public void testCreatingWatchedEvent() {
               
        EnumSet<EventType> allTypes = EnumSet.allOf(EventType.class);
       EnumSet<KeeperState> allStates = EnumSet.allOf(KeeperState.class);
       WatchedEvent we;

        for(EventType et : allTypes) {
           for(KeeperState ks : allStates) {
               we = new WatchedEvent(et, ks, "blah");
               Assert.assertEquals(et, we.getType());
               Assert.assertEquals(ks, we.getState());
               Assert.assertEquals("blah", we.getPath());
           }
        }
    }

    @Test
    public void testCreatingWatchedEventFromWrapper() {
        
        EnumSet<EventType> allTypes = EnumSet.allOf(EventType.class);
       EnumSet<KeeperState> allStates = EnumSet.allOf(KeeperState.class);
       WatchedEvent we;
       WatcherEvent wep;

        for(EventType et : allTypes) {
           for(KeeperState ks : allStates) {
               wep = new WatcherEvent(et.getIntValue(), ks.getIntValue(), "blah");
               we = new WatchedEvent(wep);
               Assert.assertEquals(et, we.getType());
               Assert.assertEquals(ks, we.getState());
               Assert.assertEquals("blah", we.getPath());
           }
        }
    }

    @Test
    public void testCreatingWatchedEventFromInvalidWrapper() {
        
       try {
           WatcherEvent wep = new WatcherEvent(-2342, -252352, "foo");
           new WatchedEvent(wep);
           Assert.fail("Was able to create WatchedEvent from bad wrapper");
       } catch (RuntimeException re) {
                  }
    }

   @Test
   public void testConvertingToEventWrapper() {
       WatchedEvent we = new WatchedEvent(EventType.NodeCreated, KeeperState.Expired, "blah");
       WatcherEvent wew = we.getWrapper();

       Assert.assertEquals(EventType.NodeCreated.getIntValue(), wew.getType());
       Assert.assertEquals(KeeperState.Expired.getIntValue(), wew.getState());
       Assert.assertEquals("blah", wew.getPath());
   }
}
