package com.ip2location.nifi.api;

public class IP2LocationLookupException extends Exception {
    public IP2LocationLookupException(final String message) {
        super(message);
    }

    public IP2LocationLookupException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
