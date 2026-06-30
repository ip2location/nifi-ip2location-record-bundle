package com.ip2location.nifi.service;

import com.ip2location.IP2Location;
import com.ip2location.IPResult;
import com.ip2location.nifi.api.IP2LocationDatabaseService;
import com.ip2location.nifi.api.IP2LocationField;
import com.ip2location.nifi.api.IP2LocationLookupException;
import com.ip2location.nifi.api.IP2LocationLookupResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"ip2location", "ip", "geo", "geolocation", "database", "bin"})
@CapabilityDescription("Opens an IP2Location BIN database and provides reusable IP address lookup results to IP2Location processors.")
public class StandardIP2LocationDatabaseService extends AbstractControllerService implements IP2LocationDatabaseService {

    private static final AllowableValue FILE_IO = new AllowableValue(
            "file-io",
            "File I/O",
            "Use normal file I/O to read the BIN database during IP lookups."
    );

    private static final AllowableValue MEMORY_MAPPED_FILE = new AllowableValue(
            "memory-mapped-file",
            "Memory Mapped File",
            "Use memory-mapped file access to read the BIN database during IP lookups when supported by the IP2Location Java library."
    );

    public static final PropertyDescriptor BIN_FILE = new PropertyDescriptor.Builder()
            .name("ip2location-bin-file")
            .displayName("IP2Location BIN File")
            .description("Absolute path to the IP2Location BIN database file. In a NiFi cluster, this file must exist at the same path on every node.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATABASE_ACCESS_METHOD = new PropertyDescriptor.Builder()
            .name("database-access-method")
            .displayName("Database Access Method")
            .description("Controls how the IP2Location Java library reads the BIN database during IP lookups. File I/O uses normal file access. Memory Mapped File can improve lookup speed, but may use more system resources depending on the operating system and database size.")
            .required(true)
            .allowableValues(FILE_IO, MEMORY_MAPPED_FILE)
            .defaultValue(FILE_IO.getValue())
            .build();

    public static final PropertyDescriptor LOOKUP_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("lookup-cache-size")
            .displayName("Lookup Cache Size")
            .description("Maximum number of repeated IP lookup results to keep in a small LRU cache. Use 0 to disable.")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(BIN_FILE, DATABASE_ACCESS_METHOD, LOOKUP_CACHE_SIZE);

    private final AtomicReference<IP2Location> databaseRef = new AtomicReference<>();
    private volatile Map<String, IP2LocationLookupResult> lookupCache = Collections.emptyMap();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String binFile = context.getProperty(BIN_FILE).getValue();
        final boolean useMemoryMappedFile = MEMORY_MAPPED_FILE.getValue().equals(context.getProperty(DATABASE_ACCESS_METHOD).getValue());
        final int cacheSize = context.getProperty(LOOKUP_CACHE_SIZE).asInteger();
        this.lookupCache = cacheSize > 0 ? synchronizedLruCache(cacheSize) : Collections.emptyMap();

        final IP2Location database = new IP2Location();
        try {
            openDatabase(database, binFile, useMemoryMappedFile);
            databaseRef.set(database);
            getLogger().info(
                    "IP2Location BIN database opened [{}] using database access method [{}]",
                    binFile,
                    context.getProperty(DATABASE_ACCESS_METHOD).getValue()
            );
        } catch (final Exception e) {
            throw new InitializationException("Failed to open IP2Location BIN database: " + binFile, e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        final IP2Location database = databaseRef.getAndSet(null);
        if (database != null) {
            closeDatabaseQuietly(database);
        }
        lookupCache.clear();
    }

    @Override
    public IP2LocationLookupResult lookup(final String ipAddress) throws IP2LocationLookupException {
        if (ipAddress == null || ipAddress.trim().isEmpty()) {
            return new IP2LocationLookupResult("NO_IP", Map.of());
        }

        final String ip = ipAddress.trim();
        if (!lookupCache.isEmpty()) {
            final IP2LocationLookupResult cached = lookupCache.get(ip);
            if (cached != null) {
                return cached;
            }
        }

        final IP2LocationLookupResult result = query(ip);
        if (!lookupCache.isEmpty()) {
            lookupCache.put(ip, result);
        }
        return result;
    }

    private IP2LocationLookupResult query(final String ip) throws IP2LocationLookupException {
        final IP2Location database = databaseRef.get();
        if (database == null) {
            throw new IP2LocationLookupException("IP2Location database service is not enabled");
        }

        final IPResult ipResult;
        try {
            // Conservative synchronization: avoids relying on thread-safety guarantees across library versions.
            synchronized (database) {
                ipResult = database.IPQuery(ip);
            }
        } catch (final Exception e) {
            throw new IP2LocationLookupException("Failed to query IP2Location database for IP: " + ip, e);
        }

        final String status = Optional.ofNullable(readRawValue(ipResult, IP2LocationField.STATUS.getterMethodName()))
                .map(Object::toString)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .orElse("UNKNOWN")
                .toUpperCase(Locale.ROOT);

        final Map<String, Object> values = new LinkedHashMap<>();
        for (final IP2LocationField field : IP2LocationField.values()) {
            if (field == IP2LocationField.STATUS) {
                continue;
            }
            values.put(field.outputName(), normalizeValue(field, readRawValue(ipResult, field.getterMethodName())));
        }
        return new IP2LocationLookupResult(status, values);
    }

    private static Object readRawValue(final IPResult ipResult, final String getterMethodName) {
        try {
            final Method method = ipResult.getClass().getMethod(getterMethodName);
            return method.invoke(ipResult);
        } catch (final ReflectiveOperationException e) {
            return null;
        }
    }

    private static Object normalizeValue(final IP2LocationField field, final Object value) {
        if (value == null) {
            return null;
        }
        if (field.fieldKind() == IP2LocationField.FieldKind.DOUBLE) {
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            final String string = value.toString().trim();
            if (string.isEmpty() || "-".equals(string)) {
                return null;
            }
            try {
                return Double.parseDouble(string);
            } catch (final NumberFormatException e) {
                return null;
            }
        }
        return value.toString();
    }

    private static void openDatabase(final IP2Location database, final String binFile, final boolean useMemoryMappedFile) throws Exception {
        try {
            final Method open = database.getClass().getMethod("Open", String.class, boolean.class);
            open.invoke(database, binFile, useMemoryMappedFile);
        } catch (final NoSuchMethodException e) {
            // Fallback for older IP2Location Java library versions that only expose Open(String).
            database.Open(binFile);
        }
    }

    private static void closeDatabaseQuietly(final IP2Location database) {
        try {
            final Method close = database.getClass().getMethod("Close");
            close.invoke(database);
        } catch (final Exception ignored) {
            // Older IP2Location Java versions may not expose Close().
        }
    }

    private static Map<String, IP2LocationLookupResult> synchronizedLruCache(final int maxEntries) {
        return Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, IP2LocationLookupResult> eldest) {
                return size() > maxEntries;
            }
        });
    }
}
