package org.apache.zookeeper.test;

import java.io.File;

import org.junit.Assert;


public class TestUtils {

    
    public static boolean deleteFileRecursively(File file,
            final boolean failOnError) {
        if (file != null) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                int size = files.length;
                for (int i = 0; i < size; i++) {
                    File f = files[i];
                    boolean deleted = deleteFileRecursively(files[i], failOnError);
                    if(!deleted && failOnError)
                    {
                        Assert.fail("file '" + f.getAbsolutePath()+"' deletion failed");
                    }
                }
            }
            return file.delete();
        }
        return true;
    }

    public static boolean deleteFileRecursively(File file) {
        return deleteFileRecursively(file, false);
    }
}
