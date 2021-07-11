package org.apache.zookeeper.test;

import java.util.EnumSet;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.Assert;
import org.junit.Test;

public class KeeperStateTest extends ZKTestCase {
    
    @Test
    public void testIntConversion() {
                EnumSet<KeeperState> allStates = EnumSet.allOf(KeeperState.class);

        for(KeeperState as : allStates) {
            Assert.assertEquals(as, KeeperState.fromInt( as.getIntValue() ) );
        }
    }

    @Test
    public void testInvalidIntConversion() {
        try {
            KeeperState.fromInt(324142);
            Assert.fail("Was able to create an invalid KeeperState via an integer");
        } catch(RuntimeException re) {
                    }

    }

    
    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedCodeOkInSwitch() {
        int test = 1;
        switch (test) {
        case Code.Ok:
            Assert.assertTrue(true);
            break;
        }
    }

    
    @Test
    public void testCodeOKInSwitch() {
        Code test = Code.OK;
        switch (test) {
        case OK:
            Assert.assertTrue(true);
            break;
        }
    }
}
