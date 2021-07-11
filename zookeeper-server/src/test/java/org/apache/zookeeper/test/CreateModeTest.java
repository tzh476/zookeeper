package org.apache.zookeeper.test;

import java.util.EnumSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.KeeperException.Code;
import org.junit.Assert;
import org.junit.Test;

public class CreateModeTest extends ZKTestCase {
    
    @Test
    public void testBasicCreateMode() {
        CreateMode cm = CreateMode.PERSISTENT;
        Assert.assertEquals(cm.toFlag(), 0);
        Assert.assertFalse(cm.isEphemeral());
        Assert.assertFalse(cm.isSequential());
        Assert.assertFalse(cm.isContainer());

        cm = CreateMode.EPHEMERAL;
        Assert.assertEquals(cm.toFlag(), 1);
        Assert.assertTrue(cm.isEphemeral());
        Assert.assertFalse(cm.isSequential());
        Assert.assertFalse(cm.isContainer());

        cm = CreateMode.PERSISTENT_SEQUENTIAL;
        Assert.assertEquals(cm.toFlag(), 2);
        Assert.assertFalse(cm.isEphemeral());
        Assert.assertTrue(cm.isSequential());
        Assert.assertFalse(cm.isContainer());

        cm = CreateMode.EPHEMERAL_SEQUENTIAL;
        Assert.assertEquals(cm.toFlag(), 3);
        Assert.assertTrue(cm.isEphemeral());
        Assert.assertTrue(cm.isSequential());
        Assert.assertFalse(cm.isContainer());

        cm = CreateMode.CONTAINER;
        Assert.assertEquals(cm.toFlag(), 4);
        Assert.assertFalse(cm.isEphemeral());
        Assert.assertFalse(cm.isSequential());
        Assert.assertTrue(cm.isContainer());
    }
    
    @Test
    public void testFlagConversion() throws KeeperException {
                EnumSet<CreateMode> allModes = EnumSet.allOf(CreateMode.class);

        for(CreateMode cm : allModes) {
            Assert.assertEquals(cm, CreateMode.fromFlag( cm.toFlag() ) );
        }
    }

    @Test
    public void testInvalidFlagConversion() throws KeeperException {
        try {
            CreateMode.fromFlag(99);
            Assert.fail("Shouldn't be able to convert 99 to a CreateMode.");
        } catch(KeeperException ke) {
            Assert.assertEquals(Code.BADARGUMENTS, ke.code());
        }

        try {
            CreateMode.fromFlag(-1);
            Assert.fail("Shouldn't be able to convert -1 to a CreateMode.");
        } catch(KeeperException ke) {
            Assert.assertEquals(Code.BADARGUMENTS, ke.code());
        }
    }
}
