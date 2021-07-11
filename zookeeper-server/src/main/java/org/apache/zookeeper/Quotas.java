package org.apache.zookeeper;


public class Quotas {

    
    public static final String procZookeeper = "/zookeeper";

    
    public static final String quotaZookeeper = "/zookeeper/quota";

    
    public static final String limitNode = "zookeeper_limits";

    
    public static final String statNode = "zookeeper_stats";

    
    public static String quotaPath(String path) {
        return quotaZookeeper + path +
        "/" + limitNode;
    }

    
    public static String statPath(String path) {
        return quotaZookeeper + path + "/" +
        statNode;
    }
}
