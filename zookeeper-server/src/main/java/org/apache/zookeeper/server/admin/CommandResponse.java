package org.apache.zookeeper.server.admin;

import java.util.LinkedHashMap;
import java.util.Map;


public class CommandResponse {

    
    public static final String KEY_COMMAND = "command";
    
    public static final String KEY_ERROR = "error";

    private final String command;
    private final String error;
    private final Map<String, Object> data;

    
    public CommandResponse(String command) {
        this(command, null);
    }
    
    public CommandResponse(String command, String error) {
        this.command = command;
        this.error = error;
        data = new LinkedHashMap<String, Object>();
    }

    
    public String getCommand() {
        return command;
    }

    
    public String getError() {
        return error;
    }

    
    public Object put(String key, Object value) {
        return data.put(key, value);
    }

    
    public void putAll(Map<? extends String,?> m) {
        data.putAll(m);
    }

    
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<String, Object>(data);
        m.put(KEY_COMMAND, command);
        m.put(KEY_ERROR, error);
        m.putAll(data);
        return m;
    }
}
