package org.apache.zookeeper.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MBeanRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(MBeanRegistry.class);
    
    private static volatile MBeanRegistry instance = new MBeanRegistry();
    
    private final Object LOCK = new Object();
    
    private Map<ZKMBeanInfo, String> mapBean2Path =
        new ConcurrentHashMap<ZKMBeanInfo, String>();
    
    private MBeanServer mBeanServer;

    
    public static void setInstance(MBeanRegistry instance) {
        MBeanRegistry.instance = instance;
    }

    public static MBeanRegistry getInstance() {
        return instance;
    }

    public MBeanRegistry () {
        try {
            mBeanServer = ManagementFactory.getPlatformMBeanServer();        
        } catch (Error e) {
                                    mBeanServer =  MBeanServerFactory.createMBeanServer();
        }
    }

    
    public MBeanServer getPlatformMBeanServer() {
        return mBeanServer;
    }

    
    public void register(ZKMBeanInfo bean, ZKMBeanInfo parent)
        throws JMException
    {
        assert bean != null;
        String path = null;
        if (parent != null) {
            path = mapBean2Path.get(parent);
            assert path != null;
        }
        path = makeFullPath(path, parent);
        if(bean.isHidden())
            return;
        ObjectName oname = makeObjectName(path, bean);
        try {
            synchronized (LOCK) {
                mBeanServer.registerMBean(bean, oname);
                mapBean2Path.put(bean, path);
            }
        } catch (JMException e) {
            LOG.warn("Failed to register MBean " + bean.getName());
            throw e;
        }
    }

    
    private void unregister(String path,ZKMBeanInfo bean) throws JMException  {
        if(path==null)
            return;
        if (!bean.isHidden()) {
            final ObjectName objName = makeObjectName(path, bean);
            LOG.debug("Unregister MBean [{}]", objName);
            synchronized (LOCK) {
               mBeanServer.unregisterMBean(objName);
            }
        }        
    }
    
    
    public Set<ZKMBeanInfo> getRegisteredBeans() {
        return new HashSet<ZKMBeanInfo>(mapBean2Path.keySet());
    }

    
    public void unregister(ZKMBeanInfo bean) {
        if(bean==null)
            return;
        String path = mapBean2Path.remove(bean);
        try {
            unregister(path,bean);
        } catch (JMException e) {
            LOG.warn("Error during unregister of [{}]", bean.getName(), e);
        } catch (Throwable t) {
            LOG.error("Unexpected exception during unregister of [{}]. It should be reviewed and fixed.", bean.getName(), t);
        }
    }

    
    public String makeFullPath(String prefix, String... name) {
        StringBuilder sb=new StringBuilder(prefix == null ? "/" : (prefix.equals("/")?prefix:prefix+"/"));
        boolean first=true;
        for (String s : name) {
            if(s==null) continue;
            if(!first){
                sb.append("/");
            }else
                first=false;
            sb.append(s);
        }
        return sb.toString();
    }
    
    protected String makeFullPath(String prefix, ZKMBeanInfo bean) {
        return makeFullPath(prefix, bean == null ? null : bean.getName());
    }

    
    private int tokenize(StringBuilder sb, String path, int index){
        String[] tokens = path.split("/");
        for (String s: tokens) {
            if (s.length()==0)
                continue;
            sb.append("name").append(index++)
                    .append("=").append(s).append(",");
        }
        return index;
    }
    
    protected ObjectName makeObjectName(String path, ZKMBeanInfo bean)
        throws MalformedObjectNameException
    {
        if(path==null)
            return null;
        StringBuilder beanName = new StringBuilder(CommonNames.DOMAIN + ":");
        int counter=0;
        counter=tokenize(beanName,path,counter);
        tokenize(beanName,bean.getName(),counter);
        beanName.deleteCharAt(beanName.length()-1);
        try {
            return new ObjectName(beanName.toString());
        } catch (MalformedObjectNameException e) {
            LOG.warn("Invalid name \"" + beanName.toString() + "\" for class "
                    + bean.getClass().toString());
            throw e;
        }
    }
}
