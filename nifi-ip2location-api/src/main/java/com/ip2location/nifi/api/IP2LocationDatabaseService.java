package com.ip2location.nifi.api;

import org.apache.nifi.controller.ControllerService;

/**
 * Shared Controller Service API for IP2Location BIN database lookups.
 */
public interface IP2LocationDatabaseService extends ControllerService {
    IP2LocationLookupResult lookup(String ipAddress) throws IP2LocationLookupException;
}
