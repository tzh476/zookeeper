package org.apache.zookeeper.test;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.common.Time;

public class TestHammer implements VoidCallback {

    
    static int REPS = 50000;
    public static void main(String[] args) {
            long startTime = Time.currentElapsedTime();
            ZooKeeper zk = null;
            try {
                zk = ClientBase.createZKClient(args[0], 10000);
            } catch (Exception e1) {
                e1.printStackTrace();
                throw new RuntimeException(e1);
            }
            for(int i = 0; i < REPS; i++) {
                try {
                    String name = zk.create("/testFile-", new byte[16], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                    zk.delete(name, -1, new TestHammer(), null);
                } catch(Exception e) {
                    i--;
                    e.printStackTrace();
                }
            }
            System.out.println("creates/sec=" + (REPS*1000/(Time.currentElapsedTime()-startTime)));
    }

    public void processResult(int rc, String path, Object ctx) {
        
    }

}
