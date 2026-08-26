/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.convert;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.GeometryOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.JsonOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.MapOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.MetadataField;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.NullValueHandling;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.VectorOutputMode;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.connector.elasticsearch.record.SinkOperation;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.time.Conversions;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

/**
 * Converts a Connect {@link Struct} with its schema, or a schemaless value, into a JSON-ready
 * document body, walking the schema and converting by logical type name. This is the fidelity
 * gap against generic JSON writers, closed by explicit handling: decimals arrive as numbers
 * rather than base64, temporals as ISO-8601, JSON columns as embedded objects, geometry as
 * GeoJSON, and vectors as arrays.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 6.1"
 */
public class DocumentConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentConverter.class);

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendLiteral('Z')
            .toFormatter();

    private final ElasticsearchSinkConnectorConfig config;
    private final AtomicBoolean schemalessReported = new AtomicBoolean();

    public DocumentConverter(ElasticsearchSinkConnectorConfig config) {
        this.config = config;
    }

    /**
     * Builds the document body for a write operation, applying the field filter, the logical
     * type registry, null handling, field name adjustment, and Kafka metadata projection.
     *
     * @throws RecordProcessingException on a value that cannot form a valid document
     */
    public Map<String, Object> convert(SinkOperation.Write write) {
        final DebeziumSinkRecord record = write.record();
        final Map<String, Object> document;
        if (write.payload() != null) {
            document = convertTopLevelStruct(record, write.payload());
        }
        else {
            document = convertSchemaless(record, write.rawValue());
        }
        projectMetadata(record, document);
        return document;
    }

    private Map<String, Object> convertTopLevelStruct(DebeziumSinkRecord record, Struct struct) {
        final Map<String, Object> result = new LinkedHashMap<>();
        for (Field field : struct.schema().fields()) {
            if (config.fieldFilter() != null && !config.fieldFilter().matches(record.topicName(), field.name())) {
                continue;
            }
            putField(result, field.name(), struct.get(field), field.schema());
        }
        return result;
    }

    private Map<String, Object> convertSchemaless(DebeziumSinkRecord record, Object value) {
        if (schemalessReported.compareAndSet(false, true)) {
            LOGGER.info("Record values without a schema detected (topic '{}'); the logical type conversions of the "
                    + "'*.output.mode' properties require a schema and are not applicable to these records.", record.topicName());
        }
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                final String name = String.valueOf(entry.getKey());
                if (config.fieldFilter() != null && !config.fieldFilter().matches(record.topicName(), name)) {
                    continue;
                }
                putField(result, name, convertRaw(entry.getValue()), null);
            }
            return result;
        }
        throw new RecordProcessingException(String.format(
                "A schemaless record value of type %s cannot form a document body (an object is required) at topic '%s' "
                        + "partition %s offset %s.",
                value == null ? "null" : value.getClass().getSimpleName(), record.topicName(), record.partition(), record.offset()));
    }

    private void putField(Map<String, Object> target, String name, Object value, Schema schema) {
        if (value == null) {
            if (config.nullValueHandling() == NullValueHandling.WRITE_NULL) {
                target.put(adjustName(name), null);
            }
            return;
        }
        target.put(adjustName(name), schema != null ? convertValue(schema, value) : convertRaw(value));
    }

    private Object convertValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        final String logicalName = schema.name();
        if (logicalName != null) {
            final Object converted = convertLogical(logicalName, schema, value);
            if (converted != CONTINUE) {
                return converted;
            }
        }
        return switch (schema.type()) {
            case STRUCT -> convertStruct((Struct) value);
            case ARRAY -> convertArray(schema, (List<?>) value);
            case MAP -> convertMap(schema, (Map<?, ?>) value);
            case BYTES -> convertBytes(toBytes(value));
            case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING -> value;
        };
    }

    /** Sentinel distinguishing "not a handled logical type" from a legitimate null conversion. */
    private static final Object CONTINUE = new Object();

    private Object convertLogical(String logicalName, Schema schema, Object value) {
        return switch (logicalName) {
            case Decimal.LOGICAL_NAME -> convertDecimal((BigDecimal) value);
            case VariableScaleDecimal.LOGICAL_NAME -> {
                final SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) value);
                yield decimal.getDecimalValue().map(this::convertDecimal).orElse(null);
            }
            case Date.SCHEMA_NAME -> convertDate(LocalDate.ofEpochDay(((Number) value).longValue()));
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                convertDate(((java.util.Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate());
            case Time.SCHEMA_NAME -> convertTime(LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Number) value).longValue())));
            case MicroTime.SCHEMA_NAME -> convertTime(LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(((Number) value).longValue())));
            case NanoTime.SCHEMA_NAME -> convertTime(LocalTime.ofNanoOfDay(((Number) value).longValue()));
            case org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                convertTime(((java.util.Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime());
            case Timestamp.SCHEMA_NAME -> convertTimestamp(Conversions.toInstantFromMillis(((Number) value).longValue()));
            case MicroTimestamp.SCHEMA_NAME -> convertTimestamp(Conversions.toInstantFromMicros(((Number) value).longValue()));
            case NanoTimestamp.SCHEMA_NAME -> convertTimestamp(Instant.ofEpochSecond(0, ((Number) value).longValue()));
            case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME -> convertTimestamp(((java.util.Date) value).toInstant());
            case ZonedTimestamp.SCHEMA_NAME, ZonedTime.SCHEMA_NAME -> value;
            case Year.SCHEMA_NAME -> ((Number) value).intValue();
            case Interval.SCHEMA_NAME -> value;
            case MicroDuration.SCHEMA_NAME -> convertMicroDuration(((Number) value).doubleValue());
            case Json.LOGICAL_NAME -> convertJson((String) value);
            // "io.debezium.data.Ltree" is defined by the Postgres connector, referenced by name.
            case Xml.LOGICAL_NAME, Uuid.LOGICAL_NAME, Enum.LOGICAL_NAME, "io.debezium.data.Ltree" ->
                value.toString();
            case EnumSet.LOGICAL_NAME -> List.of(value.toString().split(","));
            case Bits.LOGICAL_NAME -> convertBits(schema, toBytes(value));
            case Point.LOGICAL_NAME -> convertPoint((Struct) value);
            case Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME -> convertGeometry((Struct) value);
            case FloatVector.LOGICAL_NAME, DoubleVector.LOGICAL_NAME -> convertDenseVector((List<?>) value);
            case SparseDoubleVector.LOGICAL_NAME -> convertSparseVector((Struct) value);
            default -> CONTINUE;
        };
    }

    private Object convertDecimal(BigDecimal value) {
        return switch (config.decimalOutputMode()) {
            case STRING -> value.toPlainString();
            case DOUBLE -> value.doubleValue();
            case NUMERIC -> fitsDouble(value) ? value : value.toPlainString();
        };
    }

    private boolean fitsDouble(BigDecimal value) {
        final double asDouble = value.doubleValue();
        return Double.isFinite(asDouble) && BigDecimal.valueOf(asDouble).compareTo(value) == 0;
    }

    private Object convertDate(LocalDate date) {
        return switch (config.temporalOutputMode()) {
            case ISO8601 -> date.toString();
            case EPOCH_MILLIS -> TimeUnit.DAYS.toMillis(date.toEpochDay());
            case EPOCH_NANOS -> TimeUnit.DAYS.toNanos(date.toEpochDay());
        };
    }

    private Object convertTime(LocalTime time) {
        return switch (config.temporalOutputMode()) {
            case ISO8601 -> TIME_FORMAT.format(time);
            case EPOCH_MILLIS -> TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
            case EPOCH_NANOS -> time.toNanoOfDay();
        };
    }

    private Object convertTimestamp(Instant instant) {
        return switch (config.temporalOutputMode()) {
            case ISO8601 -> TIMESTAMP_FORMAT.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
            case EPOCH_MILLIS -> instant.toEpochMilli();
            case EPOCH_NANOS -> TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
        };
    }

    private Object convertMicroDuration(double micros) {
        return switch (config.intervalOutputMode()) {
            case NUMERIC -> micros;
            case STRING -> Duration.ofNanos((long) (micros * 1_000)).toString();
        };
    }

    private Object convertJson(String json) {
        if (config.jsonOutputMode() == JsonOutputMode.STRING) {
            return json;
        }
        try {
            return JSON.readValue(json, Object.class);
        }
        catch (Exception e) {
            throw new RecordProcessingException("A JSON column value could not be parsed for embedding; "
                    + "set 'json.output.mode=string' to write it as a string instead", e);
        }
    }

    private Object convertBits(Schema schema, byte[] bits) {
        return switch (config.bitsOutputMode()) {
            case BASE64 -> Base64.getEncoder().encodeToString(bits);
            case BOOLEAN_ARRAY -> {
                final int length = bitsLength(schema, bits);
                final List<Boolean> result = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    result.add((bits[i / 8] >> (i % 8) & 1) == 1);
                }
                yield result;
            }
            case INTEGER -> {
                final int length = bitsLength(schema, bits);
                if (length > 64) {
                    throw new RecordProcessingException(String.format(
                            "'%s=integer' supports bit widths up to 64, but the field is %d bits wide.",
                            ElasticsearchSinkConnectorConfig.BITS_OUTPUT_MODE, length));
                }
                long result = 0;
                for (int i = bits.length - 1; i >= 0; i--) {
                    result = (result << 8) | (bits[i] & 0xFF);
                }
                yield result;
            }
        };
    }

    private int bitsLength(Schema schema, byte[] bits) {
        final String parameter = schema.parameters() != null ? schema.parameters().get(Bits.LENGTH_FIELD) : null;
        return parameter != null ? Integer.parseInt(parameter) : bits.length * 8;
    }

    private Object convertPoint(Struct point) {
        if (config.geometryOutputMode() == GeometryOutputMode.WKB) {
            return convertGeometry(point);
        }
        final Map<String, Object> geoJson = new LinkedHashMap<>();
        geoJson.put("type", "Point");
        geoJson.put("coordinates", List.of(point.getFloat64(Point.X_FIELD),
                point.getFloat64(Point.Y_FIELD)));
        return geoJson;
    }

    private Object convertGeometry(Struct geometry) {
        final byte[] wkb = geometry.getBytes(Geometry.WKB_FIELD);
        if (config.geometryOutputMode() == GeometryOutputMode.WKB) {
            final Map<String, Object> result = new LinkedHashMap<>();
            result.put("wkb", wkb != null ? Base64.getEncoder().encodeToString(wkb) : null);
            final Object srid = geometry.schema().field(Geometry.SRID_FIELD) != null
                    ? geometry.get(Geometry.SRID_FIELD)
                    : null;
            if (srid != null) {
                result.put("srid", srid);
            }
            return result;
        }
        if (wkb == null) {
            return null;
        }
        try {
            return WkbToGeoJson.convert(wkb);
        }
        catch (RuntimeException e) {
            throw new RecordProcessingException("A geometry value could not be converted to GeoJSON; "
                    + "set 'geometry.output.mode=wkb' to write the raw WKB instead", e);
        }
    }

    private Object convertDenseVector(List<?> vector) {
        return config.vectorOutputMode() == VectorOutputMode.STRING
                ? vector.toString()
                : vector;
    }

    private Object convertSparseVector(Struct sparse) {
        final Map<?, ?> entries = (Map<?, ?>) sparse.get(SparseDoubleVector.VECTOR_FIELD);
        final Map<String, Object> result = new LinkedHashMap<>();
        entries.forEach((index, weight) -> result.put(String.valueOf(index), weight));
        return config.vectorOutputMode() == VectorOutputMode.STRING
                ? result.toString()
                : result;
    }

    private Map<String, Object> convertStruct(Struct struct) {
        final Map<String, Object> result = new LinkedHashMap<>();
        for (Field field : struct.schema().fields()) {
            final Object value = struct.get(field);
            if (value == null) {
                if (config.nullValueHandling() == NullValueHandling.WRITE_NULL) {
                    result.put(adjustName(field.name()), null);
                }
                continue;
            }
            result.put(adjustName(field.name()), convertValue(field.schema(), value));
        }
        return result;
    }

    private List<Object> convertArray(Schema schema, List<?> values) {
        final List<Object> result = new ArrayList<>(values.size());
        for (Object value : values) {
            result.add(value == null ? null : convertValue(schema.valueSchema(), value));
        }
        return result;
    }

    private Object convertMap(Schema schema, Map<?, ?> map) {
        final boolean stringKeys = schema.keySchema() != null && schema.keySchema().type() == Schema.Type.STRING;
        if (stringKeys && config.mapOutputMode() == MapOutputMode.COMPACT) {
            final Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, value) -> result.put(adjustName(String.valueOf(key)),
                    value == null ? null : convertValue(schema.valueSchema(), value)));
            return result;
        }
        final List<Object> entries = new ArrayList<>(map.size());
        map.forEach((key, value) -> {
            final Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("key", key == null ? null : convertValue(schema.keySchema(), key));
            entry.put("value", value == null ? null : convertValue(schema.valueSchema(), value));
            entries.add(entry);
        });
        return entries;
    }

    private Object convertBytes(byte[] bytes) {
        return switch (config.binaryOutputMode()) {
            case BASE64 -> Base64.getEncoder().encodeToString(bytes);
            case HEX -> HexFormat.of().formatHex(bytes);
        };
    }

    private Object convertRaw(Object value) {
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, nested) -> result.put(adjustName(String.valueOf(key)), nested == null ? null : convertRaw(nested)));
            return result;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(item -> item == null ? null : convertRaw(item)).toList();
        }
        if (value instanceof ByteBuffer buffer) {
            return convertBytes(toBytes(buffer));
        }
        if (value instanceof byte[] bytes) {
            return convertBytes(bytes);
        }
        return value;
    }

    private void projectMetadata(DebeziumSinkRecord record, Map<String, Object> document) {
        if (config.documentMetadataFields().isEmpty()) {
            return;
        }
        final String prefix = config.documentMetadataPrefix();
        for (MetadataField field : config.documentMetadataFields()) {
            final Object value = switch (field) {
                case TOPIC -> record.topicName();
                case PARTITION -> record.partition();
                case OFFSET -> record.offset();
                case TIMESTAMP -> record instanceof KafkaDebeziumSinkRecord kafkaRecord
                        ? kafkaRecord.getOriginalKafkaRecord().timestamp()
                        : null;
                case KEY -> convertKey(record);
            };
            if (value != null) {
                document.put(prefix + field.getValue(), value);
            }
        }
    }

    private Object convertKey(DebeziumSinkRecord record) {
        final Object key = record.key();
        if (key == null) {
            return null;
        }
        if (key instanceof Struct structKey) {
            return convertStruct(structKey);
        }
        if (key instanceof ByteBuffer buffer) {
            return convertBytes(toBytes(buffer));
        }
        if (key instanceof byte[] bytes) {
            return convertBytes(bytes);
        }
        return key;
    }

    /**
     * Reads the dotted routing field path from the converted document, for {@code _routing}.
     */
    public String routingValue(DebeziumSinkRecord record, Map<String, Object> document, String path) {
        Object current = document;
        for (String segment : path.split("\\.")) {
            final Object next = current instanceof Map<?, ?> map ? map.get(segment) : null;
            if (next != null) {
                current = next;
            }
            else {
                throw new RecordProcessingException(String.format(
                        "The routing field '%s' is absent from the document for record at topic '%s' partition %s offset %s; "
                                + "routing silently defaulting to '_id' would place the document on the wrong shard.",
                        path, record.topicName(), record.partition(), record.offset()));
            }
        }
        return current.toString();
    }

    private String adjustName(String name) {
        return switch (config.fieldNameAdjustmentMode()) {
            case NONE -> name;
            case ELASTICSEARCH -> name.replace(".", config.fieldNameSeparatorReplacement());
            case AVRO -> avroAdjust(name, false);
            case AVRO_UNICODE -> avroAdjust(name, true);
        };
    }

    private String avroAdjust(String name, boolean unicode) {
        final StringBuilder result = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            final boolean valid = c == '_' || Character.isLetter(c) && c < 128 || Character.isDigit(c) && c < 128 && i > 0;
            if (valid) {
                result.append(c);
            }
            else if (unicode) {
                result.append("_u").append(Integer.toHexString(c));
            }
            else {
                result.append('_');
            }
        }
        return result.toString();
    }

    private byte[] toBytes(Object value) {
        if (value instanceof ByteBuffer buffer) {
            final ByteBuffer duplicate = buffer.duplicate();
            final byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
}
