package org.apache.zookeeper.server.persistence;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class TxnLogToolkitTest {
    private static final File testData = new File(
            System.getProperty("test.data.dir", "src/test/resources/data"));

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private File mySnapDir;

    @Before
    public void setUp() throws IOException {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        File snapDir = new File(testData, "invalidsnap");
        mySnapDir = ClientBase.createTmpDir();
        FileUtils.copyDirectory(snapDir, mySnapDir);
    }

    @After
    public void tearDown() throws IOException {
        System.setOut(System.out);
        System.setErr(System.err);
        mySnapDir.setWritable(true);
        FileUtils.deleteDirectory(mySnapDir);
    }

    @Test
    public void testDumpMode() throws Exception {
                File logfile = new File(new File(mySnapDir, "version-2"), "log.274");
        TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);

                lt.dump(null);

                    }

    @Test(expected = TxnLogToolkit.TxnLogToolkitException.class)
    public void testInitMissingFile() throws FileNotFoundException, TxnLogToolkit.TxnLogToolkitException {
                File logfile = new File("this_file_should_not_exists");
        TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);
    }

    @Test(expected = TxnLogToolkit.TxnLogToolkitException.class)
    public void testInitWithRecoveryFileExists() throws IOException, TxnLogToolkit.TxnLogToolkitException {
                File logfile = new File(new File(mySnapDir, "version-2"), "log.274");
        File recoveryFile = new File(new File(mySnapDir, "version-2"), "log.274.fixed");
        recoveryFile.createNewFile();
        TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), true);
    }

    @Test
    public void testDumpWithCrcError() throws Exception {
                File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(false, false, logfile.toString(), true);

                lt.dump(null);

                String output = outContent.toString();
        Pattern p = Pattern.compile("^CRC ERROR.*session 0x8061fac5ddeb0000 cxid 0x0 zxid 0x8800000002 createSession 30000$", Pattern.MULTILINE);
        Matcher m = p.matcher(output);
        assertTrue("Output doesn't indicate CRC error for the broken session id: " + output, m.find());
    }

    @Test
    public void testRecoveryFixBrokenFile() throws Exception {
                File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), true);

                lt.dump(null);

                String output = outContent.toString();
        assertThat(output, containsString("CRC FIXED"));

                outContent.reset();
        logfile = new File(new File(mySnapDir, "version-2"), "log.42.fixed");
        lt = new TxnLogToolkit(false, false, logfile.toString(), true);
        lt.dump(null);
        output = outContent.toString();
        assertThat(output, not(containsString("CRC ERROR")));
    }

    @Test
    public void testRecoveryInteractiveMode() throws Exception {
                File logfile = new File(new File(mySnapDir, "version-2"), "log.42");
        TxnLogToolkit lt = new TxnLogToolkit(true, false, logfile.toString(), false);

                lt.dump(new Scanner("y\n"));

                String output = outContent.toString();
        assertThat(output, containsString("CRC ERROR"));

                outContent.reset();
        logfile = new File(new File(mySnapDir, "version-2"), "log.42.fixed");
        lt = new TxnLogToolkit(false, false, logfile.toString(), true);
        lt.dump(null);
        output = outContent.toString();
        assertThat(output, not(containsString("CRC ERROR")));
    }
}
