package org.apache.zookeeper.server.admin;

import java.io.PrintWriter;
import java.util.Map;


public interface CommandOutputter {
    
    String getContentType();

    void output(CommandResponse response, PrintWriter pw);
}
