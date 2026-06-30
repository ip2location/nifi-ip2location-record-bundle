package com.ip2location.nifi.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class IP2LocationLookupResult {
    private final String status;
    private final Map<String, Object> values;

    public IP2LocationLookupResult(final String status, final Map<String, Object> values) {
        this.status = status == null || status.isBlank() ? "UNKNOWN" : status.trim();
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNullElseGet(values, Map::of)));
    }

    public String getStatus() {
        return status;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public boolean isOk() {
        return "OK".equalsIgnoreCase(status);
    }

    public Map<String, Object> toSelectedMap(final List<IP2LocationField> fields,
                                             final boolean includeStatus,
                                             final boolean includeEmptyValues) {
        final Map<String, Object> selected = new LinkedHashMap<>();
        if (includeStatus) {
            selected.put(IP2LocationField.STATUS.outputName(), status);
        }
        for (final IP2LocationField field : fields) {
            if (field == IP2LocationField.STATUS) {
                continue;
            }
            final Object value = values.get(field.outputName());
            if (includeEmptyValues || isMeaningful(value)) {
                selected.put(field.outputName(), value);
            }
        }
        return selected;
    }

    private static boolean isMeaningful(final Object value) {
        if (value == null) {
            return false;
        }
        final String string = value.toString().trim();
        return !string.isEmpty() && !"-".equals(string) && !"N/A".equalsIgnoreCase(string);
    }

    public String normalizedStatus() {
        return status.toUpperCase(Locale.ROOT);
    }
}
