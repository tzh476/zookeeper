package org.apache.zookeeper.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.AtomicFileOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AtomicFileOutputStreamTest extends ZKTestCase {
    private static final String TEST_STRING = "hello world";
    private static final String TEST_STRING_2 = "goodbye world";

    private File testDir;
    private File dstFile;

    @Before
    public void setupTestDir() throws IOException {
        testDir = ClientBase.createEmptyTestDir();
        dstFile = new File(testDir, "test.txt");
    }
    @After
    public void cleanupTestDir() throws IOException {
        ClientBase.recursiveDelete(testDir);
    }

    
    @Test
    public void testWriteNewFile() throws IOException {
        OutputStream fos = new AtomicFileOutputStream(dstFile);
        assertFalse(dstFile.exists());
        fos.write(TEST_STRING.getBytes());
        fos.flush();
        assertFalse(dstFile.exists());
        fos.close();
        assertTrue(dstFile.exists());

        String readBackData = ClientBase.readFile(dstFile);
        assertEquals(TEST_STRING, readBackData);
    }

    
    @Test
    public void testOverwriteFile() throws IOException {
        assertTrue("Creating empty dst file", dstFile.createNewFile());

        OutputStream fos = new AtomicFileOutputStream(dstFile);

        assertTrue("Empty file still exists", dstFile.exists());
        fos.write(TEST_STRING.getBytes());
        fos.flush();

                assertEquals("", ClientBase.readFile(dstFile));

        fos.close();

                String readBackData = ClientBase.readFile(dstFile);
        assertEquals(TEST_STRING, readBackData);
    }

    
    @Test
    public void testFailToFlush() throws IOException {
                FileOutputStream fos = new FileOutputStream(dstFile);
        fos.write(TEST_STRING_2.getBytes());
        fos.close();

        OutputStream failingStream = createFailingStream();
        failingStream.write(TEST_STRING.getBytes());
        try {
            failingStream.close();
            fail("Close didn't throw exception");
        } catch (IOException ioe) {
                    }

                assertEquals(TEST_STRING_2, ClientBase.readFile(dstFile));

        assertEquals("Temporary file should have been cleaned up",
                dstFile.getName(), ClientBase.join(",", testDir.list()));
    }

    
    private OutputStream createFailingStream() throws FileNotFoundException {
        return new AtomicFileOutputStream(dstFile) {
            @Override
            public void flush() throws IOException {
                throw new IOException("injected failure");
            }
        };
    }

    
    @Test
    public void testAbortNewFile() throws IOException {
        AtomicFileOutputStream fos = new AtomicFileOutputStream(dstFile);

        fos.abort();

        assertEquals(0, testDir.list().length);
    }

    
    @Test
    public void testAbortNewFileAfterFlush() throws IOException {
        AtomicFileOutputStream fos = new AtomicFileOutputStream(dstFile);
        fos.write(TEST_STRING.getBytes());
        fos.flush();

        fos.abort();

        assertEquals(0, testDir.list().length);
    }

    
    @Test
    public void testAbortExistingFile() throws IOException {
        FileOutputStream fos1 = new FileOutputStream(dstFile);
        fos1.write(TEST_STRING.getBytes());
        fos1.close();

        AtomicFileOutputStream fos2 = new AtomicFileOutputStream(dstFile);

        fos2.abort();

                assertEquals(TEST_STRING, ClientBase.readFile(dstFile));
        assertEquals(1, testDir.list().length);
    }

    
    @Test
    public void testAbortExistingFileAfterFlush() throws IOException {
        FileOutputStream fos1 = new FileOutputStream(dstFile);
        fos1.write(TEST_STRING.getBytes());
        fos1.close();

        AtomicFileOutputStream fos2 = new AtomicFileOutputStream(dstFile);
        fos2.write(TEST_STRING_2.getBytes());
        fos2.flush();

        fos2.abort();

                assertEquals(TEST_STRING, ClientBase.readFile(dstFile));
        assertEquals(1, testDir.list().length);
    }
}
