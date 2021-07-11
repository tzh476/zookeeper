package org.apache.zookeeper.test;

import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.util.OSMXBean;

public class OSMXBeanTest extends ZKTestCase {
    
    private OSMXBean osMbean;
    private Long ofdc = 0L;
    private Long mfdc = 0L;
    protected static final Logger LOG = LoggerFactory.getLogger(OSMXBeanTest.class);

    @Before
    public void initialize() {
        this.osMbean = new OSMXBean();
        Assert.assertNotNull("Could not initialize OSMXBean object!", osMbean);
    }
    
    @Test
    public final void testGetUnix() {
        boolean isUnix = osMbean.getUnix();
        if (!isUnix) {
        	LOG.info("Running in a Windows system! Output won't be printed!");
        } else {
        	LOG.info("Running in a Unix or Linux system!");
        }
    }

    @Test
    public final void testGetOpenFileDescriptorCount() {
        if (osMbean != null && osMbean.getUnix() == true) {
            ofdc = osMbean.getOpenFileDescriptorCount();
            LOG.info("open fdcount is: " + ofdc);
        }   
        Assert.assertFalse("The number of open file descriptor is negative",(ofdc < 0));
    }

    @Test
    public final void testGetMaxFileDescriptorCount() {
        if (osMbean != null && osMbean.getUnix() == true) {
            mfdc = osMbean.getMaxFileDescriptorCount();
            LOG.info("max fdcount is: " + mfdc);
        }
        Assert.assertFalse("The max file descriptor number is negative",(mfdc < 0));
    }

}
