package org.apache.zookeeper.cli;

import java.io.PrintStream;
import java.util.Date;
import org.apache.zookeeper.data.Stat;


public class StatPrinter {

    protected PrintStream out;

    public StatPrinter(PrintStream out) {
        this.out = out;
    }

    public void print(Stat stat) {
        out.println("cZxid = 0x" + Long.toHexString(stat.getCzxid()));
        out.println("ctime = " + new Date(stat.getCtime()).toString());
        out.println("mZxid = 0x" + Long.toHexString(stat.getMzxid()));
        out.println("mtime = " + new Date(stat.getMtime()).toString());
        out.println("pZxid = 0x" + Long.toHexString(stat.getPzxid()));
        out.println("cversion = " + stat.getCversion());
        out.println("dataVersion = " + stat.getVersion());
        out.println("aclVersion = " + stat.getAversion());
        out.println("ephemeralOwner = 0x"
                + Long.toHexString(stat.getEphemeralOwner()));
        out.println("dataLength = " + stat.getDataLength());
        out.println("numChildren = " + stat.getNumChildren());
    }
}
