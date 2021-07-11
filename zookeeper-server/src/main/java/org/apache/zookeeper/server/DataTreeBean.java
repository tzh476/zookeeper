package org.apache.zookeeper.server;

import org.apache.zookeeper.jmx.ZKMBeanInfo;


public class DataTreeBean implements DataTreeMXBean, ZKMBeanInfo {
    DataTree dataTree;
    
    public DataTreeBean(org.apache.zookeeper.server.DataTree dataTree){
        this.dataTree = dataTree;
    }
    
    public int getNodeCount() {
        return dataTree.getNodeCount();
    }

    public long approximateDataSize() {
        return dataTree.approximateDataSize();
    }

    public int countEphemerals() {
        return dataTree.getEphemeralsCount();
    }

    public int getWatchCount() {
        return dataTree.getWatchCount();
    }

    public String getName() {
        return "InMemoryDataTree";
    }

    public boolean isHidden() {
        return false;
    }

    public String getLastZxid() {
        return "0x" + Long.toHexString(dataTree.lastProcessedZxid);
    }

}
