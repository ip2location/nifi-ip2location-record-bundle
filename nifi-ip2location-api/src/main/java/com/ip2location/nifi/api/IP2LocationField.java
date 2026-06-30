package com.ip2location.nifi.api;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * IP2Location fields that can be returned by the enrichment processor.
 *
 * The IP2Location IPResult class exposes values through public getter methods
 * such as getCountryShort(), getCity(), and getLatitude(). Its backing fields
 * are not public, so these enum values store getter method names instead of
 * Java field names.
 */
public enum IP2LocationField {
    STATUS("status", "getStatus", FieldKind.STRING),
    COUNTRY_SHORT("country_short", "getCountryShort", FieldKind.STRING),
    COUNTRY_LONG("country_long", "getCountryLong", FieldKind.STRING),
    REGION("region", "getRegion", FieldKind.STRING),
    CITY("city", "getCity", FieldKind.STRING),
    ISP("isp", "getISP", FieldKind.STRING),
    LATITUDE("latitude", "getLatitude", FieldKind.DOUBLE),
    LONGITUDE("longitude", "getLongitude", FieldKind.DOUBLE),
    DOMAIN("domain", "getDomain", FieldKind.STRING),
    ZIPCODE("zipcode", "getZipCode", FieldKind.STRING),
    TIMEZONE("timezone", "getTimeZone", FieldKind.STRING),
    NETSPEED("netspeed", "getNetSpeed", FieldKind.STRING),
    IDDCODE("iddcode", "getIDDCode", FieldKind.STRING),
    AREACODE("areacode", "getAreaCode", FieldKind.STRING),
    WEATHERSTATIONCODE("weatherstationcode", "getWeatherStationCode", FieldKind.STRING),
    WEATHERSTATIONNAME("weatherstationname", "getWeatherStationName", FieldKind.STRING),
    MCC("mcc", "getMCC", FieldKind.STRING),
    MNC("mnc", "getMNC", FieldKind.STRING),
    MOBILEBRAND("mobilebrand", "getMobileBrand", FieldKind.STRING),
    ELEVATION("elevation", "getElevation", FieldKind.DOUBLE),
    USAGETYPE("usagetype", "getUsageType", FieldKind.STRING),
    ADDRESSTYPE("addresstype", "getAddressType", FieldKind.STRING),
    CATEGORY("category", "getCategory", FieldKind.STRING),
    DISTRICT("district", "getDistrict", FieldKind.STRING),
    ASN("asn", "getASN", FieldKind.STRING),
    AS("as", "getAS", FieldKind.STRING),
    ASDOMAIN("asdomain", "getASDomain", FieldKind.STRING),
    ASUSAGETYPE("asusagetype", "getASUsageType", FieldKind.STRING),
    ASCIDR("ascidr", "getASCIDR", FieldKind.STRING);

    public enum FieldKind { STRING, DOUBLE }

    private static final Map<String, IP2LocationField> BY_NAME = new LinkedHashMap<>();
    static {
        for (final IP2LocationField field : values()) {
            BY_NAME.put(field.outputName, field);
        }
    }

    private final String outputName;
    private final String getterMethodName;
    private final FieldKind fieldKind;

    IP2LocationField(final String outputName, final String getterMethodName, final FieldKind fieldKind) {
        this.outputName = outputName;
        this.getterMethodName = getterMethodName;
        this.fieldKind = fieldKind;
    }

    public String outputName() {
        return outputName;
    }

    public String getterMethodName() {
        return getterMethodName;
    }

    public FieldKind fieldKind() {
        return fieldKind;
    }

    public static Optional<IP2LocationField> from(final String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_NAME.get(name.trim().toLowerCase(Locale.ROOT)));
    }

    public static List<IP2LocationField> parseList(final String configuredValue, final boolean includeStatus) {
        final Set<IP2LocationField> fields = new LinkedHashSet<>();
        if (includeStatus) {
            fields.add(STATUS);
        }

        final String value = configuredValue == null ? "" : configuredValue.trim();
        if ("all".equalsIgnoreCase(value)) {
            fields.addAll(Arrays.asList(values()));
            return List.copyOf(fields);
        }

        for (final String token : value.split(",")) {
            final String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                from(trimmed).ifPresent(fields::add);
            }
        }
        return List.copyOf(fields);
    }

    public static List<String> invalidNames(final String configuredValue) {
        if (configuredValue == null || configuredValue.trim().isEmpty() || "all".equalsIgnoreCase(configuredValue.trim())) {
            return List.of();
        }
        return Arrays.stream(configuredValue.split(","))
                .map(String::trim)
                .filter(token -> !token.isEmpty())
                .filter(token -> from(token).isEmpty())
                .toList();
    }
}
