package org.apache.zookeeper.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import io.netty.buffer.ByteBufAllocator;
import org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.zookeeper.server.NettyServerCnxnFactory;


public class TestByteBufAllocatorTestHelper {
    public static void setTestAllocator(ByteBufAllocator allocator)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method m1 = NettyServerCnxnFactory.class.getDeclaredMethod("setTestAllocator", ByteBufAllocator.class);
        m1.setAccessible(true);
        m1.invoke(null, allocator);
        Method m2 = ClientCnxnSocketNetty.class.getDeclaredMethod("setTestAllocator", ByteBufAllocator.class);
        m2.setAccessible(true);
        m2.invoke(null, allocator);
    }

    public static void clearTestAllocator()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method m1 = NettyServerCnxnFactory.class.getDeclaredMethod("clearTestAllocator");
        m1.setAccessible(true);
        m1.invoke(null);
        Method m2 = ClientCnxnSocketNetty.class.getDeclaredMethod("clearTestAllocator");
        m2.setAccessible(true);
        m2.invoke(null);
    }
}