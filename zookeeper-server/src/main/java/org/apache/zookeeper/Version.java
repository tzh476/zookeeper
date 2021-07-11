package org.apache.zookeeper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class Version implements org.apache.zookeeper.version.Info {

    
    @Deprecated
    public static int getRevision() {
        return REVISION;
    }

    public static String getRevisionHash() {
        return REVISION_HASH;
    }

    public static String getBuildDate() {
        return BUILD_DATE;
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE",
            justification = "Missing QUALIFIER causes redundant null-check")
    public static String getVersion() {
        return MAJOR + "." + MINOR + "." + MICRO
            + (QUALIFIER == null ? "" : "-" + QUALIFIER);
    }

    public static String getVersionRevision() {
        return getVersion() + "-" + getRevisionHash();
    }

    public static String getFullVersion() {
        return getVersionRevision() + ", built on " + getBuildDate();
    }

    public static void printUsage() {
        System.out
                .print("Usage:\tjava -cp ... org.apache.zookeeper.Version "
                        + "[--full | --short | --revision],\n\tPrints --full version "
                        + "info if no arg specified.");
        System.exit(1);
    }

    
    public static void main(String[] args) {
        if (args.length > 1) {
            printUsage();
        }
        if (args.length == 0 || (args.length == 1 && args[0].equals("--full"))) {
            System.out.println(getFullVersion());
            System.exit(0);
        }
        if (args[0].equals("--short"))
            System.out.println(getVersion());
        else if (args[0].equals("--revision"))
            System.out.println(getVersionRevision());
        else
            printUsage();
        System.exit(0);
    }
}
