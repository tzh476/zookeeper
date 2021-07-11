package org.apache.zookeeper.server;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

public class DataNodeTest {

    @Test
    public void testGetChildrenShouldReturnEmptySetWhenThereAreNoChidren() {
                DataNode dataNode = new DataNode();
        Set<String> children = dataNode.getChildren();
        assertNotNull(children);
        assertEquals(0, children.size());

                String child = "child";
        dataNode.addChild(child);
        dataNode.removeChild(child);
        children = dataNode.getChildren();
        assertNotNull(children);
        assertEquals(0, children.size());

                children = dataNode.getChildren();
        try {
            children.add("new child");
            fail("UnsupportedOperationException is expected");
        } catch (UnsupportedOperationException e) {
                    }
    }

    @Test
    public void testGetChildrenReturnsImmutableEmptySet() {
        DataNode dataNode = new DataNode();
        Set<String> children = dataNode.getChildren();
        try {
            children.add("new child");
            fail("UnsupportedOperationException is expected");
        } catch (UnsupportedOperationException e) {
                    }
    }
}
