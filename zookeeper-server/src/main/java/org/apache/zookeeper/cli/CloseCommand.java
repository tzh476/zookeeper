package org.apache.zookeeper.cli;


public class CloseCommand extends CliCommand {

    public CloseCommand() {
        super("close", "");
    }
    
    
    @Override
    public CliCommand parse(String[] cmdArgs) throws CliParseException {
        return this;
    }

    @Override
    public boolean exec() throws CliException {
        try {
            zk.close();
        } catch (Exception ex) {
            throw new CliWrapperException(ex);
        }
        
        return false;
    }
    
}
