package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;


@InterfaceAudience.Public
public interface Watcher {

    
    @InterfaceAudience.Public
    public interface Event {
        
        @InterfaceAudience.Public
        public enum KeeperState {
            
            @Deprecated
            Unknown (-1),

            
            Disconnected (0),

            
            @Deprecated
            NoSyncConnected (1),

            
            SyncConnected (3),

            
            AuthFailed (4),

            
            ConnectedReadOnly (5),

            
            SaslAuthenticated(6),

            
            Expired (-112),
            
            
            Closed (7);

            private final int intValue;                                                 
            KeeperState(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static KeeperState fromInt(int intValue) {
                switch(intValue) {
                    case   -1: return KeeperState.Unknown;
                    case    0: return KeeperState.Disconnected;
                    case    1: return KeeperState.NoSyncConnected;
                    case    3: return KeeperState.SyncConnected;
                    case    4: return KeeperState.AuthFailed;
                    case    5: return KeeperState.ConnectedReadOnly;
                    case    6: return KeeperState.SaslAuthenticated;
                    case -112: return KeeperState.Expired;
                    case   7: return KeeperState.Closed;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to KeeperState");
                }
            }
        }

        
        @InterfaceAudience.Public
        public enum EventType {
            None (-1),
            NodeCreated (1),
            NodeDeleted (2),
            NodeDataChanged (3),
            NodeChildrenChanged (4),
            DataWatchRemoved (5),
            ChildWatchRemoved (6);

            private final int intValue;                                                 
            EventType(int intValue) {
                this.intValue = intValue;
            }

            public int getIntValue() {
                return intValue;
            }

            public static EventType fromInt(int intValue) {
                switch(intValue) {
                    case -1: return EventType.None;
                    case  1: return EventType.NodeCreated;
                    case  2: return EventType.NodeDeleted;
                    case  3: return EventType.NodeDataChanged;
                    case  4: return EventType.NodeChildrenChanged;
                    case  5: return EventType.DataWatchRemoved;
                    case  6: return EventType.ChildWatchRemoved;

                    default:
                        throw new RuntimeException("Invalid integer value for conversion to EventType");
                }
            }           
        }
    }

    
    @InterfaceAudience.Public
    public enum WatcherType {
        Children(1), Data(2), Any(3);

                private final int intValue;

        private WatcherType(int intValue) {
            this.intValue = intValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public static WatcherType fromInt(int intValue) {
            switch (intValue) {
            case 1:
                return WatcherType.Children;
            case 2:
                return WatcherType.Data;
            case 3:
                return WatcherType.Any;

            default:
                throw new RuntimeException(
                        "Invalid integer value for conversion to WatcherType");
            }
        }
    }

    abstract public void process(WatchedEvent event);
}
