package org.apache.zookeeper.server.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;


public class FourLetterCommands {
    
    public final static int confCmd =
        ByteBuffer.wrap("conf".getBytes()).getInt();

    
    public final static int consCmd =
        ByteBuffer.wrap("cons".getBytes()).getInt();

    
    public final static int crstCmd =
        ByteBuffer.wrap("crst".getBytes()).getInt();

    
    public final static int dirsCmd =
        ByteBuffer.wrap("dirs".getBytes()).getInt();

    
    public final static int dumpCmd =
        ByteBuffer.wrap("dump".getBytes()).getInt();

    
    public final static int enviCmd =
        ByteBuffer.wrap("envi".getBytes()).getInt();

    
    public final static int getTraceMaskCmd =
        ByteBuffer.wrap("gtmk".getBytes()).getInt();

    
    public final static int ruokCmd =
        ByteBuffer.wrap("ruok".getBytes()).getInt();
    
    public final static int setTraceMaskCmd =
        ByteBuffer.wrap("stmk".getBytes()).getInt();

    
    public final static int srvrCmd =
        ByteBuffer.wrap("srvr".getBytes()).getInt();

    
    public final static int srstCmd =
        ByteBuffer.wrap("srst".getBytes()).getInt();

    
    public final static int statCmd =
        ByteBuffer.wrap("stat".getBytes()).getInt();

    
    public final static int wchcCmd =
        ByteBuffer.wrap("wchc".getBytes()).getInt();

    
    public final static int wchpCmd =
        ByteBuffer.wrap("wchp".getBytes()).getInt();

    
    public final static int wchsCmd =
        ByteBuffer.wrap("wchs".getBytes()).getInt();

    
    public final static int mntrCmd = ByteBuffer.wrap("mntr".getBytes())
            .getInt();

    
    public final static int isroCmd = ByteBuffer.wrap("isro".getBytes())
            .getInt();

    
    public final static int telnetCloseCmd = 0xfff4fffd;

    private static final String ZOOKEEPER_4LW_COMMANDS_WHITELIST = "zookeeper.4lw.commands.whitelist";

    private static final Logger LOG = LoggerFactory.getLogger(FourLetterCommands.class);

    private static final Map<Integer, String> cmd2String = new HashMap<Integer, String>();

    private static final Set<String> whiteListedCommands = new HashSet<String>();

    private static boolean whiteListInitialized = false;

        public synchronized static void resetWhiteList() {
        whiteListInitialized = false;
        whiteListedCommands.clear();
    }

    
    public static String getCommandString(int command) {
        return cmd2String.get(command);
    }

    
    public static boolean isKnown(int command) {
        return cmd2String.containsKey(command);
    }

    
    public synchronized static boolean isEnabled(String command) {
        if (whiteListInitialized) {
            return whiteListedCommands.contains(command);
        }

        String commands = System.getProperty(ZOOKEEPER_4LW_COMMANDS_WHITELIST);
        if (commands != null) {
            String[] list = commands.split(",");
            for (String cmd : list) {
                if (cmd.trim().equals("*")) {
                    for (Map.Entry<Integer, String> entry : cmd2String.entrySet()) {
                        whiteListedCommands.add(entry.getValue());
                    }
                    break;
                }
                if (!cmd.trim().isEmpty()) {
                    whiteListedCommands.add(cmd.trim());
                }
            }
        }

                        if (System.getProperty("readonlymode.enabled", "false").equals("true")) {
            whiteListedCommands.add("isro");
        }
                whiteListedCommands.add("srvr");
        whiteListInitialized = true;
        LOG.info("The list of known four letter word commands is : {}", Arrays.asList(cmd2String));
        LOG.info("The list of enabled four letter word commands is : {}", Arrays.asList(whiteListedCommands));
        return whiteListedCommands.contains(command);
    }

        static {
        cmd2String.put(confCmd, "conf");
        cmd2String.put(consCmd, "cons");
        cmd2String.put(crstCmd, "crst");
        cmd2String.put(dirsCmd, "dirs");
        cmd2String.put(dumpCmd, "dump");
        cmd2String.put(enviCmd, "envi");
        cmd2String.put(getTraceMaskCmd, "gtmk");
        cmd2String.put(ruokCmd, "ruok");
        cmd2String.put(setTraceMaskCmd, "stmk");
        cmd2String.put(srstCmd, "srst");
        cmd2String.put(srvrCmd, "srvr");
        cmd2String.put(statCmd, "stat");
        cmd2String.put(wchcCmd, "wchc");
        cmd2String.put(wchpCmd, "wchp");
        cmd2String.put(wchsCmd, "wchs");
        cmd2String.put(mntrCmd, "mntr");
        cmd2String.put(isroCmd, "isro");
        cmd2String.put(telnetCloseCmd, "telnet close");
    }
}
