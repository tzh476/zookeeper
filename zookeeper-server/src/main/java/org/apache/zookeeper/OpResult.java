package org.apache.zookeeper;


import org.apache.zookeeper.data.Stat;


public abstract class OpResult {
    private int type;

    private OpResult(int type) {
        this.type = type;
    }

    
    public int getType() {
        return type;
    }

    
    public static class CreateResult extends OpResult {
        private String path;
        private Stat stat;

        public CreateResult(String path) {
        	this(ZooDefs.OpCode.create, path, null);
        }

        public CreateResult(String path, Stat stat) {
            this(ZooDefs.OpCode.create2, path, stat);
        }

        private CreateResult(int opcode, String path, Stat stat) {
        	super(opcode);
            this.path = path;
            this.stat = stat;
        }

        public String getPath() {
            return path;
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CreateResult)) return false;

            CreateResult other = (CreateResult) o;

            boolean statsAreEqual = (stat == null && other.stat == null ||
                        						(stat != null && other.stat != null &&
                        					   stat.getMzxid() == other.stat.getMzxid()));
            return getType() == other.getType() &&
                   path.equals(other.getPath()) && statsAreEqual;
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + path.hashCode() +
                    (stat == null ? 0 : stat.getMzxid()));
        }
    }

    
    public static class DeleteResult extends OpResult {
        public DeleteResult() {
            super(ZooDefs.OpCode.delete);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DeleteResult)) return false;

            DeleteResult opResult = (DeleteResult) o;
            return getType() == opResult.getType();
        }

        @Override
        public int hashCode() {
            return getType();
        }
    }

    
    public static class SetDataResult extends OpResult {
        private Stat stat;

        public SetDataResult(Stat stat) {
            super(ZooDefs.OpCode.setData);
            this.stat = stat;
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SetDataResult)) return false;

            SetDataResult other = (SetDataResult) o;
            return getType() == other.getType() && stat.getMzxid() == other.stat.getMzxid();
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + stat.getMzxid());
        }
    }

    
    public static class CheckResult extends OpResult {
        public CheckResult() {
            super(ZooDefs.OpCode.check);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CheckResult)) return false;

            CheckResult other = (CheckResult) o;
            return getType() == other.getType();
        }

        @Override
        public int hashCode() {
            return getType();
        }
    }

    
    public static class ErrorResult extends OpResult {
        private int err;

        public ErrorResult(int err) {
            super(ZooDefs.OpCode.error);
            this.err = err;
        }

        public int getErr() {
            return err;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ErrorResult)) return false;

            ErrorResult other = (ErrorResult) o;
            return getType() == other.getType() && err == other.getErr();
        }

        @Override
        public int hashCode() {
            return getType() * 35 + err;
        }
    }
}
