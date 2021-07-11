package org.apache.zookeeper.common;

import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

public class FileKeyStoreLoaderBuilderProviderTest extends ZKTestCase {
    @Test
    public void testGetBuilderForJKSFileType() {
        FileKeyStoreLoader.Builder<?> builder =
                FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(
                        KeyStoreFileType.JKS);
        Assert.assertTrue(builder instanceof JKSFileLoader.Builder);
    }

    @Test
    public void testGetBuilderForPEMFileType() {
        FileKeyStoreLoader.Builder<?> builder =
                FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(
                        KeyStoreFileType.PEM);
        Assert.assertTrue(builder instanceof PEMFileLoader.Builder);
    }

    @Test
    public void testGetBuilderForPKCS12FileType() {
        FileKeyStoreLoader.Builder<?> builder =
            FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(
                KeyStoreFileType.PKCS12);
        Assert.assertTrue(builder instanceof PKCS12FileLoader.Builder);
    }

    @Test(expected = NullPointerException.class)
    public void testGetBuilderForNullFileType() {
        FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(null);
    }
}
