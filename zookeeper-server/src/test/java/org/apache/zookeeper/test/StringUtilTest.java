package org.apache.zookeeper.test;


import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.common.StringUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class StringUtilTest extends ZKTestCase {

    @Test
    public void testStrings() {

        String s1 = "   a  ,   b  , ";
        assertEquals("[a, b]", StringUtils.split(s1, ",").toString());

        String s2 = "";
        assertEquals(0, StringUtils.split(s2, ",").size());

        String s3 = "1, , 2";
        assertEquals("[1, 2]", StringUtils.split(s3, ",").toString());

    }
}
