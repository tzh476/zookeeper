package org.apache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;
import org.junit.internal.runners.statements.InvokeMethod;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import java.util.Arrays;
import java.util.List;


public class JUnit4ZKTestRunner extends BlockJUnit4ClassRunner {
    private static final Logger LOG = LoggerFactory.getLogger(JUnit4ZKTestRunner.class);

    public JUnit4ZKTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @SuppressWarnings("unchecked")
    public static List<FrameworkMethod> computeTestMethodsForClass(final Class klass, final List<FrameworkMethod> defaultMethods) {
        List<FrameworkMethod> list = defaultMethods;
        String methodName = System.getProperty("test.method");
        if (methodName == null) {
            LOG.info("No test.method specified. using default methods.");
        } else {
            LOG.info("Picked up test.method={}", methodName);
            try {
                list = Arrays.asList(new FrameworkMethod(klass.getMethod(methodName)));
            } catch (NoSuchMethodException nsme) {
                LOG.warn("{} does not have test.method={}. failing to default methods.", klass.getName(), methodName);
            }
        }
        return list;
    }


    @Override
    protected List<FrameworkMethod> computeTestMethods() {
        return computeTestMethodsForClass(getTestClass().getJavaClass(), super.computeTestMethods());
    }

    public static class LoggedInvokeMethod extends InvokeMethod {
        private final FrameworkMethod method;
        private final String name;

        public LoggedInvokeMethod(FrameworkMethod method, Object target) {
            super(method, target);
            this.method = method;
            name = method.getName();
        }

        @Override
        public void evaluate() throws Throwable {
            LOG.info("RUNNING TEST METHOD {}", name);
            try {
                super.evaluate();
                Runtime rt = Runtime.getRuntime();
                long usedKB = (rt.totalMemory() - rt.freeMemory()) / 1024;
                LOG.info("Memory used {}", usedKB);
                ThreadGroup tg = Thread.currentThread().getThreadGroup();
                while (tg.getParent() != null) {
                    tg = tg.getParent();
                }
                LOG.info("Number of threads {}", tg.activeCount());
            } catch (Throwable t) {
                                                                Test annotation = this.method.getAnnotation(Test.class);
                if (annotation != null && annotation.expected() != null &&
                        annotation.expected().isAssignableFrom(t.getClass())) {
                    LOG.info("TEST METHOD {} THREW EXPECTED EXCEPTION {}", name,
                        annotation.expected());
                } else {
                    LOG.info("TEST METHOD FAILED {}", name, t);
                }
                throw t;
            }
            LOG.info("FINISHED TEST METHOD {}", name);
        }
    }

    @Override
    protected Statement methodInvoker(FrameworkMethod method, Object test) {
        return new LoggedInvokeMethod(method, test);
    }
}
