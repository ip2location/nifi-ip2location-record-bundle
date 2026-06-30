package com.ip2location.nifi.processors;

import com.ip2location.nifi.api.IP2LocationDatabaseService;
import com.ip2location.nifi.api.IP2LocationField;
import com.ip2location.nifi.api.IP2LocationLookupException;
import com.ip2location.nifi.api.IP2LocationLookupResult;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.SimpleRecordSchema;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"ip2location", "ip", "geo", "geolocation", "record", "enrich", "json", "csv", "avro", "bin"})
@CapabilityDescription("Enriches each record in a FlowFile with geolocation data from an IP2Location BIN database. "
        + "The processor uses NiFi RecordReader and RecordSetWriter services, so JSON, CSV, Avro, XML, and other record formats can be supported through normal NiFi UI configuration.")
public class IP2LocationEnrichRecord extends AbstractProcessor {

    private static final AllowableValue KEEP_RECORD = new AllowableValue("keep-record", "Keep Record", "Keep the record and add an enrichment object with status such as NO_IP or INVALID_IP_ADDRESS.");
    private static final AllowableValue LEAVE_EMPTY = new AllowableValue("leave-empty", "Leave Empty", "Keep the record but do not add the enrichment object when lookup is not OK.");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully enriched and written.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that cannot be read, enriched, or written.")
            .build();

    public static final PropertyDescriptor DATABASE_SERVICE = new PropertyDescriptor.Builder()
            .name("ip2location-database-service")
            .displayName("IP2Location Database Service")
            .description("Controller Service that opens and queries the IP2Location BIN database.")
            .required(true)
            .identifiesControllerService(IP2LocationDatabaseService.class)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Record Reader used to parse the incoming FlowFile content.")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Record Writer used to write the enriched FlowFile content.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor IP_FIELD_PATH = new PropertyDescriptor.Builder()
            .name("ip-field-path")
            .displayName("IP Field Path")
            .description("Simple slash-separated record field path containing the IP address, for example /ip or /client/ip. This intentionally supports the common field path subset rather than the full NiFi RecordPath language.")
            .required(true)
            .defaultValue("/ip")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("output-field-name")
            .displayName("Output Field Name")
            .description("Top-level field name to add or overwrite with the IP2Location enrichment object.")
            .required(true)
            .defaultValue("ip2location")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETURN_FIELDS = new PropertyDescriptor.Builder()
            .name("return-fields")
            .displayName("Return Fields")
            .description("Comma-separated IP2Location fields to include. Use 'all' for all supported fields. Common fields: country_short,country_long,region,city,latitude,longitude,zipcode,timezone,isp,asn,as,usagetype,district")
            .required(true)
            .defaultValue("country_short,country_long,region,city,latitude,longitude,zipcode,timezone,isp,asn,as,usagetype,district")
            .addValidator(IP2LocationEnrichRecord::validateReturnFields)
            .build();

    public static final PropertyDescriptor INCLUDE_STATUS = new PropertyDescriptor.Builder()
            .name("include-status")
            .displayName("Include Status")
            .description("Include IP2Location lookup status in the enrichment object.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor INCLUDE_EMPTY_VALUES = new PropertyDescriptor.Builder()
            .name("include-empty-values")
            .displayName("Include Empty Values")
            .description("When false, empty, '-', or unavailable field values are omitted from the enrichment object.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor NON_OK_STRATEGY = new PropertyDescriptor.Builder()
            .name("non-ok-strategy")
            .displayName("Non-OK Lookup Strategy")
            .description("Controls what happens per record when lookup status is not OK, such as invalid IP address or missing IP field.")
            .required(true)
            .allowableValues(KEEP_RECORD, LEAVE_EMPTY)
            .defaultValue(KEEP_RECORD.getValue())
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(
                DATABASE_SERVICE,
                RECORD_READER,
                RECORD_WRITER,
                IP_FIELD_PATH,
                OUTPUT_FIELD_NAME,
                RETURN_FIELDS,
                INCLUDE_STATUS,
                INCLUDE_EMPTY_VALUES,
                NON_OK_STRATEGY
        );
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final IP2LocationDatabaseService databaseService = context.getProperty(DATABASE_SERVICE).asControllerService(IP2LocationDatabaseService.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final String ipFieldPath = context.getProperty(IP_FIELD_PATH).getValue();
        final String outputFieldName = normalizeFieldName(context.getProperty(OUTPUT_FIELD_NAME).getValue());
        final boolean includeStatus = context.getProperty(INCLUDE_STATUS).asBoolean();
        final boolean includeEmptyValues = context.getProperty(INCLUDE_EMPTY_VALUES).asBoolean();
        final boolean keepNonOk = KEEP_RECORD.getValue().equals(context.getProperty(NON_OK_STRATEGY).getValue());
        final List<IP2LocationField> returnFields = IP2LocationField.parseList(context.getProperty(RETURN_FIELDS).getValue(), includeStatus);
        final RecordSchema enrichmentSchema = createEnrichmentSchema(returnFields);

        final AtomicReference<Map<String, String>> writerAttributes = new AtomicReference<>(Map.of());
        final AtomicInteger recordsProcessed = new AtomicInteger();
        final AtomicInteger recordsEnriched = new AtomicInteger();
        final AtomicInteger recordsNonOk = new AtomicInteger();

        try {
            final FlowFile inputFlowFile = flowFile;
            flowFile = session.write(inputFlowFile, (final InputStream inputStream, final OutputStream outputStream) -> {
                try (final RecordReader reader = readerFactory.createRecordReader(inputFlowFile, inputStream, getLogger())) {
                    final RecordSchema inputSchema = reader.getSchema();
                    final RecordSchema outputSchema = createOutputSchema(inputSchema, outputFieldName, enrichmentSchema);

                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), outputSchema, outputStream, inputFlowFile.getAttributes())) {
                        writer.beginRecordSet();

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            recordsProcessed.incrementAndGet();
                            final Record enriched = enrichRecord(record, inputSchema, outputSchema, outputFieldName, enrichmentSchema,
                                    ipFieldPath, databaseService, returnFields, includeStatus, includeEmptyValues, keepNonOk,
                                    recordsEnriched, recordsNonOk);
                            writer.write(enriched);
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        writerAttributes.set(writeResult == null ? Map.of() : writeResult.getAttributes());
                    }
                } catch (final Exception e) {
                    throw new IOException("Failed to read, enrich, or write records", e);
                }
            });

            final Map<String, String> attrs = new HashMap<>(writerAttributes.get());
            attrs.put("ip2location.records.processed", Integer.toString(recordsProcessed.get()));
            attrs.put("ip2location.records.enriched", Integer.toString(recordsEnriched.get()));
            attrs.put("ip2location.records.non_ok", Integer.toString(recordsNonOk.get()));
            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to enrich FlowFile with IP2Location data", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private Record enrichRecord(final Record inputRecord,
                                final RecordSchema inputSchema,
                                final RecordSchema outputSchema,
                                final String outputFieldName,
                                final RecordSchema enrichmentSchema,
                                final String ipFieldPath,
                                final IP2LocationDatabaseService databaseService,
                                final List<IP2LocationField> returnFields,
                                final boolean includeStatus,
                                final boolean includeEmptyValues,
                                final boolean keepNonOk,
                                final AtomicInteger recordsEnriched,
                                final AtomicInteger recordsNonOk) throws IP2LocationLookupException {

        final Map<String, Object> values = copyRecordValues(inputRecord, inputSchema);
        final Object rawIp = readSimplePath(inputRecord, ipFieldPath);
        final String ip = rawIp == null ? null : rawIp.toString().trim();
        final IP2LocationLookupResult lookupResult = databaseService.lookup(ip);

        if (!lookupResult.isOk()) {
            recordsNonOk.incrementAndGet();
            if (keepNonOk) {
                final Map<String, Object> statusOnly = new LinkedHashMap<>();
                if (includeStatus) {
                    statusOnly.put(IP2LocationField.STATUS.outputName(), lookupResult.getStatus());
                }
                values.put(outputFieldName, new MapRecord(enrichmentSchema, statusOnly));
            }
            return new MapRecord(outputSchema, values);
        }

        final Map<String, Object> enrichment = lookupResult.toSelectedMap(returnFields, includeStatus, includeEmptyValues);
        if (!enrichment.isEmpty()) {
            recordsEnriched.incrementAndGet();
            values.put(outputFieldName, new MapRecord(enrichmentSchema, enrichment));
        }
        return new MapRecord(outputSchema, values);
    }

    private static Map<String, Object> copyRecordValues(final Record record, final RecordSchema schema) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final RecordField field : schema.getFields()) {
            values.put(field.getFieldName(), record.getValue(field.getFieldName()));
        }
        return values;
    }

    private static Object readSimplePath(final Record record, final String configuredPath) {
        final String normalized = configuredPath == null ? "" : configuredPath.trim().replaceFirst("^/+", "");
        if (normalized.isEmpty()) {
            return null;
        }

        Object current = record;
        for (final String segment : normalized.split("/")) {
            if (segment.isBlank()) {
                continue;
            }
            if (current instanceof Record currentRecord) {
                current = currentRecord.getValue(segment);
            } else if (current instanceof Map<?, ?> currentMap) {
                current = currentMap.get(segment);
            } else {
                return null;
            }
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static String normalizeFieldName(final String configuredFieldName) {
        return configuredFieldName == null ? "ip2location" : configuredFieldName.trim().replaceFirst("^/+", "");
    }

    private static RecordSchema createOutputSchema(final RecordSchema inputSchema, final String outputFieldName, final RecordSchema enrichmentSchema) {
        final List<RecordField> fields = new ArrayList<>();
        final Set<String> names = new LinkedHashSet<>();
        for (final RecordField field : inputSchema.getFields()) {
            if (!field.getFieldName().equals(outputFieldName)) {
                fields.add(field);
                names.add(field.getFieldName());
            }
        }
        final DataType enrichmentType = RecordFieldType.RECORD.getRecordDataType(enrichmentSchema);
        fields.add(new RecordField(outputFieldName, enrichmentType));
        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema createEnrichmentSchema(final List<IP2LocationField> returnFields) {
        final List<RecordField> fields = returnFields.stream()
                .map(field -> new RecordField(field.outputName(), toDataType(field)))
                .collect(Collectors.toList());
        return new SimpleRecordSchema(fields);
    }

    private static DataType toDataType(final IP2LocationField field) {
        return field.fieldKind() == IP2LocationField.FieldKind.DOUBLE
                ? RecordFieldType.DOUBLE.getDataType()
                : RecordFieldType.STRING.getDataType();
    }

    private static ValidationResult validateReturnFields(final String subject, final String input, final ValidationContext context) {
        if (input == null || input.trim().isEmpty()) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("At least one field or 'all' is required").build();
        }
        final List<String> invalid = IP2LocationField.invalidNames(input);
        if (!invalid.isEmpty()) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("Unsupported field(s): " + String.join(", ", invalid)).build();
        }
        return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
    }
}
