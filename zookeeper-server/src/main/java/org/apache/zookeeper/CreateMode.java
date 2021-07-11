package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Public
public enum CreateMode {
    
    
    PERSISTENT (0, false, false, false, false),
    
    PERSISTENT_SEQUENTIAL (2, false, true, false, false),
    
    EPHEMERAL (1, true, false, false, false),
    
    EPHEMERAL_SEQUENTIAL (3, true, true, false, false),
    
    CONTAINER (4, false, false, true, false),
    
    PERSISTENT_WITH_TTL(5, false, false, false, true),
    
    PERSISTENT_SEQUENTIAL_WITH_TTL(6, false, true, false, true);

    private static final Logger LOG = LoggerFactory.getLogger(CreateMode.class);

    private boolean ephemeral;
    private boolean sequential;
    private final boolean isContainer;
    private int flag;
    private boolean isTTL;

    CreateMode(int flag, boolean ephemeral, boolean sequential,
               boolean isContainer, boolean isTTL) {
        this.flag = flag;
        this.ephemeral = ephemeral;
        this.sequential = sequential;
        this.isContainer = isContainer;
        this.isTTL = isTTL;
    }

    public boolean isEphemeral() { 
        return ephemeral;
    }

    public boolean isSequential() { 
        return sequential;
    }

    public boolean isContainer() {
        return isContainer;
    }

    public boolean isTTL() {
        return isTTL;
    }

    public int toFlag() {
        return flag;
    }

    
    static public CreateMode fromFlag(int flag) throws KeeperException {
        switch(flag) {
        case 0: return CreateMode.PERSISTENT;

        case 1: return CreateMode.EPHEMERAL;

        case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

        case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

        case 4: return CreateMode.CONTAINER;

        case 5: return CreateMode.PERSISTENT_WITH_TTL;

        case 6: return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

        default:
            String errMsg = "Received an invalid flag value: " + flag
                    + " to convert to a CreateMode";
            LOG.error(errMsg);
            throw new KeeperException.BadArgumentsException(errMsg);
        }
    }

    
    static public CreateMode fromFlag(int flag, CreateMode defaultMode) {
        switch(flag) {
            case 0:
                return CreateMode.PERSISTENT;

            case 1:
                return CreateMode.EPHEMERAL;

            case 2:
                return CreateMode.PERSISTENT_SEQUENTIAL;

            case 3:
                return CreateMode.EPHEMERAL_SEQUENTIAL;

            case 4:
                return CreateMode.CONTAINER;

            case 5:
                return CreateMode.PERSISTENT_WITH_TTL;

            case 6:
                return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

            default:
                return defaultMode;
        }
    }
}
