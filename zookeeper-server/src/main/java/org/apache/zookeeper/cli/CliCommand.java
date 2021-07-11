package org.apache.zookeeper.cli;

import java.io.PrintStream;
import java.util.Map;
import org.apache.zookeeper.ZooKeeper;


abstract public class CliCommand {
    protected ZooKeeper zk;
    protected PrintStream out;
    protected PrintStream err;
    private String cmdStr;
    private String optionStr;

    
    public CliCommand(String cmdStr, String optionStr) {
        this.out = System.out;
        this.err = System.err;
        this.cmdStr = cmdStr;
        this.optionStr = optionStr;
    }

    
    public void setOut(PrintStream out) {
        this.out = out;
    }

    
    public void setErr(PrintStream err) {
        this.err = err;
    }

    
    public void setZk(ZooKeeper zk) {
        this.zk = zk;
    }

    
    public String getCmdStr() {
        return cmdStr;
    }

    
    public String getOptionStr() {
        return optionStr;
    }

    
    public String getUsageStr() {
        return cmdStr + " " + optionStr;
    }

    
    public void addToMap(Map<String, CliCommand> cmdMap) {
        cmdMap.put(cmdStr, this);
    }
    
    
    abstract public CliCommand parse(String cmdArgs[]) throws CliParseException;
    
    
    abstract public boolean exec() throws CliException;
}
