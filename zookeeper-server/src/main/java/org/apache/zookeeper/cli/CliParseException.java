package org.apache.zookeeper.cli;

import org.apache.commons.cli.ParseException;

@SuppressWarnings("serial")
public class CliParseException extends CliException {
    public CliParseException(ParseException parseException) {
        super(parseException);
    }

    public CliParseException(String message) {
        super(message);
    }
}
