package org.apache.zookeeper;

import static org.junit.Assert.fail;
import java.time.LocalDateTime;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@SuppressWarnings("deprecation")
@RunWith(JUnit4ZKTestRunner.class)
public class ZKTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ZKTestCase.class);

    private String testName;

    protected String getTestName() {
        return testName;
    }

    @Rule
    public TestWatcher watchman= new TestWatcher() {
        
        @Override
        public void starting(Description method) {
                                                System.setProperty("zookeeper.admin.enableServer", "false");
                                    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
            testName = method.getMethodName();
            LOG.info("STARTING " + testName);
        }

        @Override
        public void finished(Description method) {
            LOG.info("FINISHED " + testName);
        }

        @Override
        public void succeeded(Description method) {
            LOG.info("SUCCEEDED " + testName);
        }

        @Override
        public void failed(Throwable e, Description method) {
            LOG.info("FAILED " + testName, e);
        }

    };
    public interface WaitForCondition {
        
        boolean evaluate();
    }

    
    public void waitFor(String msg, WaitForCondition condition, int timeout) throws InterruptedException {
        final LocalDateTime deadline = LocalDateTime.now().plusSeconds(timeout);
        while (LocalDateTime.now().isBefore(deadline)) {
            if (condition.evaluate()) {
                return;
            }
            Thread.sleep(100);
        }
        fail(msg);
    }

}
