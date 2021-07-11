package org.apache.zookeeper.server;


public enum EphemeralTypeEmulate353 {
    
    VOID,
    
    NORMAL,
    
    CONTAINER,
    
    TTL;

    public static final long CONTAINER_EPHEMERAL_OWNER = Long.MIN_VALUE;
    public static final long MAX_TTL = 0x0fffffffffffffffL;
    public static final long TTL_MASK = 0x8000000000000000L;

    public static EphemeralTypeEmulate353 get(long ephemeralOwner) {
        if (ephemeralOwner == CONTAINER_EPHEMERAL_OWNER) {
            return CONTAINER;
        }
        if (ephemeralOwner < 0) {
            return TTL;
        }
        return (ephemeralOwner == 0) ? VOID : NORMAL;
    }

    public static long ttlToEphemeralOwner(long ttl) {
        if ((ttl > MAX_TTL) || (ttl <= 0)) {
            throw new IllegalArgumentException("ttl must be positive and cannot be larger than: " + MAX_TTL);
        }
        return TTL_MASK | ttl;
    }
}
