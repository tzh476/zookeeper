package org.apache.zookeeper.test;

import java.util.EnumSet;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.Assert;
import org.junit.Test;

public class EventTypeTest extends ZKTestCase {
    
    @Test
    public void testIntConversion() {
                EnumSet<EventType> allTypes = EnumSet.allOf(EventType.class);

        for(EventType et : allTypes) {
            Assert.assertEquals(et, EventType.fromInt( et.getIntValue() ) );
        }
    }

    @Test
    public void testInvalidIntConversion() {
        try {
            EventType.fromInt(324242);
            Assert.fail("Was able to create an invalid EventType via an integer");
        } catch(RuntimeException re) {
                    }

    }
}
