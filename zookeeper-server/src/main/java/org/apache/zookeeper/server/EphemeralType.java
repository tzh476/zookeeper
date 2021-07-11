package org.apache.zookeeper.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.zookeeper.CreateMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public enum EphemeralType {
    
    VOID,
    
    NORMAL,
    
    CONTAINER,
    
    TTL() {
        @Override
        public long maxValue() {
            return EXTENDED_FEATURE_VALUE_MASK;          }

        @Override
        public long toEphemeralOwner(long ttl) {
            if ((ttl > TTL.maxValue()) || (ttl <= 0)) {
                throw new IllegalArgumentException("ttl must be positive and cannot be larger than: " + TTL.maxValue());
            }
                        return EXTENDED_MASK | EXTENDED_BIT_TTL | ttl;          }

        @Override
        public long getValue(long ephemeralOwner) {
            return getExtendedFeatureValue(ephemeralOwner);
        }
    };

    
    public long maxValue() {
        return 0;
    }

    
    public long toEphemeralOwner(long value) {
        return 0;
    }

    
    public long getValue(long ephemeralOwner) {
        return 0;
    }

    public static final long CONTAINER_EPHEMERAL_OWNER = Long.MIN_VALUE;
    public static final long MAX_EXTENDED_SERVER_ID = 0xfe;  
    private static final long EXTENDED_MASK = 0xff00000000000000L;
    private static final long EXTENDED_BIT_TTL = 0x0000;
    private static final long RESERVED_BITS_MASK = 0x00ffff0000000000L;
    private static final long RESERVED_BITS_SHIFT = 40;

    private static final Map<Long, EphemeralType> extendedFeatureMap;

    static {
        Map<Long, EphemeralType> map = new HashMap<>();
        map.put(EXTENDED_BIT_TTL, TTL);
        extendedFeatureMap = Collections.unmodifiableMap(map);
    }

    private static final long EXTENDED_FEATURE_VALUE_MASK = ~(EXTENDED_MASK | RESERVED_BITS_MASK);

        static final String EXTENDED_TYPES_ENABLED_PROPERTY = "zookeeper.extendedTypesEnabled";
    static final String TTL_3_5_3_EMULATION_PROPERTY = "zookeeper.emulate353TTLNodes";

    
    public static boolean extendedEphemeralTypesEnabled() {
        return Boolean.getBoolean(EXTENDED_TYPES_ENABLED_PROPERTY);
    }

    
    public static EphemeralType get(long ephemeralOwner) {
        if (extendedEphemeralTypesEnabled()) {
            if (Boolean.getBoolean(TTL_3_5_3_EMULATION_PROPERTY)) {
                if (EphemeralTypeEmulate353.get(ephemeralOwner) == EphemeralTypeEmulate353.TTL) {
                    return TTL;
                }
            }

            if ((ephemeralOwner & EXTENDED_MASK) == EXTENDED_MASK) {
                long extendedFeatureBit = getExtendedFeatureBit(ephemeralOwner);
                EphemeralType ephemeralType = extendedFeatureMap.get(extendedFeatureBit);
                if (ephemeralType == null) {
                    throw new IllegalArgumentException(String.format("Invalid ephemeralOwner. [%s]", Long.toHexString(ephemeralOwner)));
                }
                return ephemeralType;
            }
        }
        if (ephemeralOwner == CONTAINER_EPHEMERAL_OWNER) {
            return CONTAINER;
        }
        return (ephemeralOwner == 0) ? VOID : NORMAL;
    }

    
    public static void validateServerId(long serverId) {
                
        if (extendedEphemeralTypesEnabled()) {
            if (serverId > EphemeralType.MAX_EXTENDED_SERVER_ID) {
                throw new RuntimeException("extendedTypesEnabled is true but Server ID is too large. Cannot be larger than " + EphemeralType.MAX_EXTENDED_SERVER_ID);
            }
        }
    }

    
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "toEphemeralOwner may throw IllegalArgumentException")
    public static void validateTTL(CreateMode mode, long ttl) {
        if (mode.isTTL()) {
            TTL.toEphemeralOwner(ttl);
        } else if (ttl >= 0) {
            throw new IllegalArgumentException("ttl not valid for mode: " + mode);
        }
    }

    private static long getExtendedFeatureBit(long ephemeralOwner) {
        return (ephemeralOwner & RESERVED_BITS_MASK) >> RESERVED_BITS_SHIFT;
    }

    private static long getExtendedFeatureValue(long ephemeralOwner) {
        return ephemeralOwner & EXTENDED_FEATURE_VALUE_MASK;
    }
}
