package org.apache.zookeeper;

import org.apache.jute.Record;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.EphemeralType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public abstract class Op {
    private int type;
    private String path;

        private Op(int type, String path) {
        this.type = type;
        this.path = path;
    }

    
    public static Op create(String path, byte[] data, List<ACL> acl, int flags) {
        return new Create(path, data, acl, flags);
    }

    
    public static Op create(String path, byte[] data, List<ACL> acl, int flags, long ttl) {
        CreateMode createMode = CreateMode.fromFlag(flags, CreateMode.PERSISTENT);
        if (createMode.isTTL()) {
            return new CreateTTL(path, data, acl, createMode, ttl);
        }
        return new Create(path, data, acl, flags);
    }

    
    public static Op create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        return new Create(path, data, acl, createMode);
    }

    
    public static Op create(String path, byte[] data, List<ACL> acl, CreateMode createMode, long ttl) {
        if (createMode.isTTL()) {
            return new CreateTTL(path, data, acl, createMode, ttl);
        }
        return new Create(path, data, acl, createMode);
    }

    
    public static Op delete(String path, int version) {
        return new Delete(path, version);
    }

    
    public static Op setData(String path, byte[] data, int version) {
        return new SetData(path, data, version);
    }


    
    public static Op check(String path, int version) {
        return new Check(path, version);
    }

    
    public int getType() {
        return type;
    }

    
    public String getPath() {
        return path;
    }

    
    public abstract Record toRequestRecord() ;
    
    
    abstract Op withChroot(String addRootPrefix);

    
    void validate() throws KeeperException {
        PathUtils.validatePath(path);
    }

                public static class Create extends Op {
        protected byte[] data;
        protected List<ACL> acl;
        protected int flags;

        private Create(String path, byte[] data, List<ACL> acl, int flags) {
            super(getOpcode(CreateMode.fromFlag(flags, CreateMode.PERSISTENT)), path);
            this.data = data;
            this.acl = acl;
            this.flags = flags;
        }

        private static int getOpcode(CreateMode createMode) {
            if (createMode.isTTL()) {
                return ZooDefs.OpCode.createTTL;
            }
            return createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create;
        }

        private Create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
            super(getOpcode(createMode), path);
            this.data = data;
            this.acl = acl;
            this.flags = createMode.toFlag();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Create)) return false;

            Create op = (Create) o;

            boolean aclEquals = true;
            Iterator<ACL> i = op.acl.iterator();
            for (ACL acl : op.acl) {
                boolean hasMoreData = i.hasNext();
                if (!hasMoreData) {
                    aclEquals = false;
                    break;
                }
                ACL otherAcl = i.next();
                if (!acl.equals(otherAcl)) {
                    aclEquals = false;
                    break;
                }
            }
            return !i.hasNext() && getType() == op.getType() && Arrays.equals(data, op.data) && flags == op.flags && aclEquals;
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + Arrays.hashCode(data);
        }

        @Override
        public Record toRequestRecord() {
            return new CreateRequest(getPath(), data, acl, flags);
        }

        @Override
        Op withChroot(String path) {
            return new Create(path, data, acl, flags);
        }

        @Override
        void validate() throws KeeperException {
            CreateMode createMode = CreateMode.fromFlag(flags);
            PathUtils.validatePath(getPath(), createMode.isSequential());
            EphemeralType.validateTTL(createMode, -1);
        }
    }

    public static class CreateTTL extends Create {
        private final long ttl;

        private CreateTTL(String path, byte[] data, List<ACL> acl, int flags, long ttl) {
            super(path, data, acl, flags);
            this.ttl = ttl;
        }

        private CreateTTL(String path, byte[] data, List<ACL> acl, CreateMode createMode, long ttl) {
            super(path, data, acl, createMode);
            this.ttl = ttl;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && (o instanceof CreateTTL) && (ttl == ((CreateTTL)o).ttl);
        }

        @Override
        public int hashCode() {
            return super.hashCode() + (int)(ttl ^ (ttl >>> 32));
        }

        @Override
        public Record toRequestRecord() {
            return new CreateTTLRequest(getPath(), data, acl, flags, ttl);
        }

        @Override
        Op withChroot(String path) {
            return new CreateTTL(path, data, acl, flags, ttl);
        }

        @Override
        void validate() throws KeeperException {
            CreateMode createMode = CreateMode.fromFlag(flags);
            PathUtils.validatePath(getPath(), createMode.isSequential());
            EphemeralType.validateTTL(createMode, ttl);
        }
    }

    public static class Delete extends Op {
        private int version;

        private Delete(String path, int version) {
            super(ZooDefs.OpCode.delete, path);
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Delete)) return false;

            Delete op = (Delete) o;

            return getType() == op.getType() && version == op.version 
                   && getPath().equals(op.getPath());
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new DeleteRequest(getPath(), version);
        }

        @Override
        Op withChroot(String path) {
            return new Delete(path, version);
        }
    }

    public static class SetData extends Op {
        private byte[] data;
        private int version;

        private SetData(String path, byte[] data, int version) {
            super(ZooDefs.OpCode.setData, path);
            this.data = data;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SetData)) return false;

            SetData op = (SetData) o;

            return getType() == op.getType() && version == op.version 
                   && getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + Arrays.hashCode(data) + version;
        }

        @Override
        public Record toRequestRecord() {
            return new SetDataRequest(getPath(), data, version);
        }

        @Override
        Op withChroot(String path) {
            return new SetData(path, data, version);
        }
    }

    public static class Check extends Op {
        private int version;

        private Check(String path, int version) {
            super(ZooDefs.OpCode.check, path);
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Check)) return false;

            Check op = (Check) o;

            return getType() == op.getType() && getPath().equals(op.getPath()) && version == op.version;
        }

        @Override
        public int hashCode() {
            return getType() + getPath().hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new CheckVersionRequest(getPath(), version);
        }

        @Override
        Op withChroot(String path) {
            return new Check(path, version);
        }
    }

}
