package org.apache.zookeeper.cli;

@SuppressWarnings("serial")
public class MalformedCommandException extends CliException {
    public MalformedCommandException(String message) {
        super(message);
    }
}
