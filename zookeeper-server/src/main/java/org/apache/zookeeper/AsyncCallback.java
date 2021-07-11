package org.apache.zookeeper;

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


@InterfaceAudience.Public
public interface AsyncCallback {

    
    @InterfaceAudience.Public
    interface StatCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx, Stat stat);
    }

    
    @InterfaceAudience.Public
    interface DataCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx, byte data[],
                Stat stat);
    }

    
    @InterfaceAudience.Public
    interface ACLCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx,
                List<ACL> acl, Stat stat);
    }

    
    @InterfaceAudience.Public
    interface ChildrenCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx,
                List<String> children);
    }

    
    @InterfaceAudience.Public
    interface Children2Callback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx,
                List<String> children, Stat stat);
    }

    
    @InterfaceAudience.Public
    interface Create2Callback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx,
        		String name, Stat stat);
    }

    
    @InterfaceAudience.Public
    interface StringCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx, String name);
    }

    
    @InterfaceAudience.Public
    interface VoidCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx);
    }

    
    @InterfaceAudience.Public
    interface MultiCallback extends AsyncCallback {
        
        public void processResult(int rc, String path, Object ctx,
                List<OpResult> opResults);
    }
}
