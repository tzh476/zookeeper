package org.apache.zookeeper.server.admin;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class CommandBase implements Command {
    private final String primaryName;
    private final Set<String> names;
    private final String doc;

    
    protected CommandBase(List<String> names) {
        this(names, null);
    }

    protected CommandBase(List<String> names, String doc) {
        this.primaryName = names.get(0);
        this.names = new HashSet<String>(names);
        this.doc = doc;
    }

    @Override
    public String getPrimaryName() {
        return primaryName;
    }

    @Override
    public Set<String> getNames() {
        return names;
    }

    @Override
    public String getDoc() {
        return doc;
    }

    
    protected CommandResponse initializeResponse() {
        return new CommandResponse(primaryName);
    }
}
