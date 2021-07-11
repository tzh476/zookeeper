package org.apache.zookeeper.server.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Method;


public class OSMXBean
{
    private static final Logger LOG = LoggerFactory.getLogger(OSMXBean.class);

    private OperatingSystemMXBean osMbean;

    private static final boolean ibmvendor =
        System.getProperty("java.vendor").contains("IBM");
    private static final boolean windows = 
        System.getProperty("os.name").startsWith("Windows");
    private static final boolean linux =
        System.getProperty("os.name").startsWith("Linux");

    
    public OSMXBean () {
        this.osMbean = ManagementFactory.getOperatingSystemMXBean();
    }
 
    
    public boolean getUnix() {
        if (windows) {
            return false;
        }
        return (ibmvendor ? linux : true);
    }

    
    private Long getOSUnixMXBeanMethod (String mBeanMethodName)
    {
        Object unixos;
        Class<?> classRef;
        Method mBeanMethod;

        try {
            classRef = Class.forName("com.sun.management.UnixOperatingSystemMXBean");
            if (classRef.isInstance(osMbean)) {
                mBeanMethod = classRef.getDeclaredMethod(mBeanMethodName,
                new Class<?>[0]);
                unixos = classRef.cast(osMbean);
                return (Long)mBeanMethod.invoke(unixos);
            }
        } catch(Exception e) {
            LOG.warn("Not able to load class or method for com.sun.managment.UnixOperatingSystemMXBean.", e);
        }
        return null;
    }

    
    public long getOpenFileDescriptorCount() 
    {
        Long ofdc;
    
        if (!ibmvendor) {
            ofdc = getOSUnixMXBeanMethod("getOpenFileDescriptorCount");
            return (ofdc != null ? ofdc.longValue () : -1);
        }
        
        try {
                        RuntimeMXBean rtmbean = ManagementFactory.getRuntimeMXBean();
            String rtname = rtmbean.getName();
            String[] pidhost = rtname.split("@");

                        Process p = Runtime.getRuntime().exec(
                    new String[] { "bash", "-c",
                    "ls /proc/" + pidhost[0] + "/fdinfo | wc -l" });
            InputStream in = p.getInputStream();
            BufferedReader output = new BufferedReader(
                    new InputStreamReader(in));

            try {
                String openFileDesCount;
                if ((openFileDesCount = output.readLine()) != null) {
                    return Long.parseLong(openFileDesCount);
                }
            } finally {
                if (output != null) {
                    output.close();
                }
            }
        } catch (IOException ie) {
            LOG.warn("Not able to get the number of open file descriptors", ie);
        }
        return -1;
    }

    
    public long getMaxFileDescriptorCount()
    {
        Long mfdc;

        if (!ibmvendor) {
            mfdc = getOSUnixMXBeanMethod("getMaxFileDescriptorCount");
            return (mfdc != null ? mfdc.longValue () : -1);
        }
        
        try {
                        Process p = Runtime.getRuntime().exec(
                    new String[] { "bash", "-c", "ulimit -n" });
            InputStream in = p.getInputStream();
            BufferedReader output = new BufferedReader(
                    new InputStreamReader(in));

            try {
                String maxFileDesCount;
                if ((maxFileDesCount = output.readLine()) != null) {
                    return Long.parseLong(maxFileDesCount);
                }
            } finally {
                if (output != null) {
                    output.close();
                }
            }
        } catch (IOException ie) {
            LOG.warn("Not able to get the max number of file descriptors", ie);
        }
        return -1;
    }  
}
