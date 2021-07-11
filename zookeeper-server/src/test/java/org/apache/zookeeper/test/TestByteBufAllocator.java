package org.apache.zookeeper.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;


public class TestByteBufAllocator extends PooledByteBufAllocator {
    private static AtomicReference<TestByteBufAllocator> INSTANCE =
            new AtomicReference<>(null);

    
    public static TestByteBufAllocator getInstance() {
        TestByteBufAllocator result = INSTANCE.get();
        if (result == null) {
            ResourceLeakDetector.Level oldLevel = ResourceLeakDetector.getLevel();
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
            INSTANCE.compareAndSet(null, new TestByteBufAllocator(oldLevel));
            result = INSTANCE.get();
        }
        return result;
    }

    
    public static void checkForLeaks() {
        TestByteBufAllocator result = INSTANCE.getAndSet(null);
        if (result != null) {
            result.checkInstanceForLeaks();
        }
    }

    private final List<ByteBuf> trackedBuffers = new ArrayList<>();
    private final ResourceLeakDetector.Level oldLevel;

    private TestByteBufAllocator(ResourceLeakDetector.Level oldLevel)
    {
        super(false);
        this.oldLevel = oldLevel;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity)
    {
        return track(super.newHeapBuffer(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity)
    {
        return track(super.newDirectBuffer(initialCapacity, maxCapacity));
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents)
    {
        return track(super.compositeHeapBuffer(maxNumComponents));
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents)
    {
        return track(super.compositeDirectBuffer(maxNumComponents));
    }

    private synchronized CompositeByteBuf track(CompositeByteBuf byteBuf)
    {
        trackedBuffers.add(Objects.requireNonNull(byteBuf));
        return byteBuf;
    }

    private synchronized ByteBuf track(ByteBuf byteBuf)
    {
        trackedBuffers.add(Objects.requireNonNull(byteBuf));
        return byteBuf;
    }

    private void checkInstanceForLeaks()
    {
        try {
            long referencedBuffersCount = 0;
            synchronized (this) {
                referencedBuffersCount = trackedBuffers.stream()
                        .filter(byteBuf -> byteBuf.refCnt() > 0)
                        .count();
                                trackedBuffers.clear();
            }
                        if (referencedBuffersCount > 0) {
                                                                System.gc();
                throw new AssertionError("Found a netty ByteBuf leak!");
            }
        } finally {
            ResourceLeakDetector.setLevel(oldLevel);
        }
    }
}