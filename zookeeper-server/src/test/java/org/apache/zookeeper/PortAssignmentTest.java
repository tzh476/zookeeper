package org.apache.zookeeper;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(ZKParameterized.RunnerFactory.class)
public class PortAssignmentTest {

    private final String strProcessCount;
    private final String cmdLine;
    private final int expectedMinimumPort;
    private final int expectedMaximumPort;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
                new Object[] { "8", "threadid=1", 11221, 13913 },
                new Object[] { "8", "threadid=2", 13914, 16606 },
                new Object[] { "8", "threadid=3", 16607, 19299 },
                new Object[] { "8", "threadid=4", 19300, 21992 },
                new Object[] { "8", "threadid=5", 21993, 24685 },
                new Object[] { "8", "threadid=6", 24686, 27378 },
                new Object[] { "8", "threadid=7", 27379, 30071 },
                new Object[] { "8", "threadid=8", 30072, 32764 },
                new Object[] { "1", "threadid=1", 11221, 32767 },
                new Object[] { "2", "threadid=1", 11221, 21993 },
                new Object[] { "2", "threadid=2", 21994, 32766 },
                new Object[] { null, null, 11221, 32767 },
                new Object[] { "", "", 11221, 32767 });
    }

    public PortAssignmentTest(String strProcessCount, String cmdLine,
            int expectedMinimumPort, int expectedMaximumPort) {
        this.strProcessCount = strProcessCount;
        this.cmdLine = cmdLine;
        this.expectedMinimumPort = expectedMinimumPort;
        this.expectedMaximumPort = expectedMaximumPort;
    }

    @Test
    public void testSetupPortRange() {
        PortAssignment.PortRange portRange = PortAssignment.setupPortRange(
                strProcessCount, cmdLine);
        assertEquals(buildAssertionMessage("minimum"), expectedMinimumPort,
                portRange.getMinimum());
        assertEquals(buildAssertionMessage("maximum"), expectedMaximumPort,
                portRange.getMaximum());
    }

    private String buildAssertionMessage(String checkType) {
        return String.format("strProcessCount = %s, cmdLine = %s, checking %s",
                strProcessCount, cmdLine, checkType);
    }
}
