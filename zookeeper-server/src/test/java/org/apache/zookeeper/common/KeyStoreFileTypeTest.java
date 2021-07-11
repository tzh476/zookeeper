package org.apache.zookeeper.common;

import org.apache.zookeeper.ZKTestCase;
import org.junit.Assert;
import org.junit.Test;

public class KeyStoreFileTypeTest extends ZKTestCase {
    @Test
    public void testGetPropertyValue() {
        Assert.assertEquals("PEM", KeyStoreFileType.PEM.getPropertyValue());
        Assert.assertEquals("JKS", KeyStoreFileType.JKS.getPropertyValue());
        Assert.assertEquals("PKCS12", KeyStoreFileType.PKCS12.getPropertyValue());
    }

    @Test
    public void testFromPropertyValue() {
        Assert.assertEquals(KeyStoreFileType.PEM, KeyStoreFileType.fromPropertyValue("PEM"));
        Assert.assertEquals(KeyStoreFileType.JKS, KeyStoreFileType.fromPropertyValue("JKS"));
        Assert.assertEquals(KeyStoreFileType.PKCS12, KeyStoreFileType.fromPropertyValue("PKCS12"));
        Assert.assertNull(KeyStoreFileType.fromPropertyValue(""));
        Assert.assertNull(KeyStoreFileType.fromPropertyValue(null));
    }

    @Test
    public void testFromPropertyValueIgnoresCase() {
        Assert.assertEquals(KeyStoreFileType.PEM, KeyStoreFileType.fromPropertyValue("pem"));
        Assert.assertEquals(KeyStoreFileType.JKS, KeyStoreFileType.fromPropertyValue("jks"));
        Assert.assertEquals(KeyStoreFileType.PKCS12, KeyStoreFileType.fromPropertyValue("pkcs12"));
        Assert.assertNull(KeyStoreFileType.fromPropertyValue(""));
        Assert.assertNull(KeyStoreFileType.fromPropertyValue(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValueThrowsOnBadPropertyValue() {
        KeyStoreFileType.fromPropertyValue("foobar");
    }

    @Test
    public void testFromFilename() {
        Assert.assertEquals(KeyStoreFileType.JKS,
                KeyStoreFileType.fromFilename("mykey.jks"));
        Assert.assertEquals(KeyStoreFileType.JKS,
                KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.jks"));
        Assert.assertEquals(KeyStoreFileType.PEM,
                KeyStoreFileType.fromFilename("mykey.pem"));
        Assert.assertEquals(KeyStoreFileType.PEM,
                KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.pem"));
        Assert.assertEquals(KeyStoreFileType.PKCS12,
            KeyStoreFileType.fromFilename("mykey.p12"));
        Assert.assertEquals(KeyStoreFileType.PKCS12,
            KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.p12"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromFilenameThrowsOnBadFileExtension() {
        KeyStoreFileType.fromFilename("prod.key");
    }

    @Test
    public void testFromPropertyValueOrFileName() {
                Assert.assertEquals(KeyStoreFileType.JKS,
                KeyStoreFileType.fromPropertyValueOrFileName(
                        "JKS", "prod.key"));
        Assert.assertEquals(KeyStoreFileType.PEM,
            KeyStoreFileType.fromPropertyValueOrFileName(
                "PEM", "prod.key"));
        Assert.assertEquals(KeyStoreFileType.PKCS12,
            KeyStoreFileType.fromPropertyValueOrFileName(
                "PKCS12", "prod.key"));
                Assert.assertEquals(KeyStoreFileType.JKS,
                KeyStoreFileType.fromPropertyValueOrFileName("", "prod.jks"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValueOrFileNameThrowsOnBadPropertyValue() {
        KeyStoreFileType.fromPropertyValueOrFileName("foobar", "prod.jks");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromPropertyValueOrFileNameThrowsOnBadFileExtension() {
        KeyStoreFileType.fromPropertyValueOrFileName("", "prod.key");
    }
}
