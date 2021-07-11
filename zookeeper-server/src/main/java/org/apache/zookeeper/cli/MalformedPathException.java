package org.apache.zookeeper.cli;

@SuppressWarnings("serial")
public class MalformedPathException extends CliException {
    public MalformedPathException(String message) {
        super(message);
    }
}
