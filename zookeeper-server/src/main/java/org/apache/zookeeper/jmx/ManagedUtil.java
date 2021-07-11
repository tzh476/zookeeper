package org.apache.zookeeper.jmx;

import java.util.Enumeration;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ManagedUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedUtil.class);

    private static boolean isLog4jJmxEnabled() {
        boolean enabled = false;

        if (Boolean.getBoolean("zookeeper.jmx.log4j.disable")) {
            LOG.info("Log4j 1.2 jmx support is disabled by property.");
        } else {
            try {
                Class.forName("org.apache.log4j.jmx.HierarchyDynamicMBean");
                enabled = true;
                LOG.info("Log4j 1.2 jmx support found and enabled.");
            } catch (ClassNotFoundException e) {
                LOG.info("Log4j 1.2 jmx support not found; jmx disabled.");
            }
        }

        return enabled;
    }


    
    @SuppressWarnings("rawtypes")
    public static void registerLog4jMBeans() throws JMException {
        if (isLog4jJmxEnabled()) {
            LOG.debug("registerLog4jMBeans()");
            MBeanServer mbs = MBeanRegistry.getInstance().getPlatformMBeanServer();

            try {
                                                Object hdm = Class.forName("org.apache.log4j.jmx.HierarchyDynamicMBean").getConstructor().newInstance();

                String mbean = System.getProperty("zookeeper.jmx.log4j.mbean", "log4j:hierarchy=default");
                ObjectName mbo = new ObjectName(mbean);
                mbs.registerMBean(hdm, mbo);

                                                                Object rootLogger = Class.forName("org.apache.log4j.Logger")
                        .getMethod("getRootLogger", (Class<?>[]) null)
                        .invoke(null, (Object[]) null);

                                Object rootLoggerName = rootLogger.getClass()
                        .getMethod("getName", (Class<?>[]) null)
                        .invoke(rootLogger, (Object[]) null);
                hdm.getClass().getMethod("addLoggerMBean", String.class)
                        .invoke(hdm, rootLoggerName);

                                                                                Object r = Class.forName("org.apache.log4j.LogManager")
                        .getMethod("getLoggerRepository", (Class<?>[]) null)
                        .invoke(null, (Object[]) null);

                                Enumeration enumer = (Enumeration) r.getClass()
                        .getMethod("getCurrentLoggers", (Class<?>[]) null)
                        .invoke(r, (Object[]) null);

                while (enumer.hasMoreElements()) {
                    Object logger = enumer.nextElement();
                                        Object loggerName = logger.getClass()
                            .getMethod("getName", (Class<?>[]) null)
                            .invoke(logger, (Object[]) null);
                    hdm.getClass().getMethod("addLoggerMBean", String.class)
                            .invoke(hdm, loggerName);
                }
            } catch (Exception e) {
                LOG.error("Problems while registering log4j 1.2 jmx beans!", e);
                throw new JMException(e.toString());
            }
        }
    }

}
