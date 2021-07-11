package org.apache.zookeeper.server.util;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.zookeeper.ZKTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerifyingFileFactoryTest extends ZKTestCase {

    private Logger log;

    @Before
    public void setUp(){
        log = LoggerFactory.getLogger("TODO: Mock Logging");
    }

    @Test
    public void testForWarningOnRelativePath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
            .warnForRelativePath().build();
        vff.create("a/relative/path");
            }

    @Test
    public void testForNoWarningOnIntendedRelativePath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
            .warnForRelativePath().build();
        vff.create("./an/intended/relative/path");
            }

    @Test(expected=IllegalArgumentException.class)
    public void testForFailForNonExistingPath() {
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log)
            .failForNonExistingPath().build();
        vff.create("/I/H0p3/this/path/d035/n0t/ex15t");
    }

    @Test
    public void testFileHasCorrectPath() {
        File file = new File("/some/path");
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(log).build();
        assertEquals(file, vff.create(file.getPath()));
    }
}
