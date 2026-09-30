/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.convert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.connector.elasticsearch.record.SinkOperation;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
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
 * The type-fidelity matrix for {@link DocumentConverter}: every logical type the registry handles
 * under each output mode, plus the cross-cutting rules of DDD-61 6.1 and 6.2.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class DocumentConverterTest {

    private static final String TOPIC = "inventory.customers";
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();

    private static final long DAY_2024_01_01 = 19723;
    private static final long MILLIS_2024_01_01 = DAY_2024_01_01 * 86_400_000L;
    private static final int MILLIS_01_02_03_456 = 3_723_456;

    @ParameterizedTest
    @CsvSource({ "numeric,12.34,NUMBER", "string,12.34,STRING", "double,12.34,DOUBLE" })
    void shouldConvertDecimalPerOutputMode(String mode, String value, String shape) {
        final Object converted = convertField(config(ElasticsearchSinkConnectorConfig.DECIMAL_OUTPUT_MODE, mode),
                Decimal.schema(2), new BigDecimal(value));
        switch (shape) {
            case "NUMBER" -> assertThat(converted).isEqualTo(new BigDecimal(value));
            case "STRING" -> assertThat(converted).isEqualTo(value);
            case "DOUBLE" -> assertThat(converted).isEqualTo(Double.parseDouble(value));
            default -> throw new IllegalArgumentException(shape);
        }
    }

    @Test
    void shouldWriteDecimalAsStringWhenItExceedsDoublePrecisionUnderNumeric() {
        final BigDecimal precise = new BigDecimal("0.12345678901234567890123");
        assertThat(convertField(config(), Decimal.schema(23), precise)).isEqualTo(precise.toPlainString());
    }

    @Test
    void shouldConvertVariableScaleDecimalPerOutputMode() {
        final Struct value = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("12.34"));
        assertThat(convertField(config(), VariableScaleDecimal.schema(), value)).isEqualTo(new BigDecimal("12.34"));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.DECIMAL_OUTPUT_MODE, "string"),
                VariableScaleDecimal.schema(), value)).isEqualTo("12.34");
    }

    @ParameterizedTest
    @CsvSource({ "iso8601,2024-01-01", "epoch_millis,1704067200000", "epoch_nanos,1704067200000000000" })
    void shouldConvertDatesPerTemporalMode(String mode, String expected) {
        final ElasticsearchSinkConnectorConfig config = config(ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, mode);
        final Object expectedValue = mode.equals("iso8601") ? expected : Long.parseLong(expected);
        assertThat(convertField(config, Date.schema(), (int) DAY_2024_01_01)).isEqualTo(expectedValue);
        assertThat(convertField(config, org.apache.kafka.connect.data.Date.SCHEMA, new java.util.Date(MILLIS_2024_01_01)))
                .isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @CsvSource({ "iso8601,01:02:03.456", "epoch_millis,3723456", "epoch_nanos,3723456000000" })
    void shouldConvertTimesPerTemporalMode(String mode, String expected) {
        final ElasticsearchSinkConnectorConfig config = config(ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, mode);
        final Object expectedValue = mode.equals("iso8601") ? expected : Long.parseLong(expected);
        assertThat(convertField(config, Time.schema(), MILLIS_01_02_03_456)).isEqualTo(expectedValue);
        assertThat(convertField(config, MicroTime.schema(), MILLIS_01_02_03_456 * 1_000L)).isEqualTo(expectedValue);
        assertThat(convertField(config, NanoTime.schema(), MILLIS_01_02_03_456 * 1_000_000L)).isEqualTo(expectedValue);
        assertThat(convertField(config, org.apache.kafka.connect.data.Time.SCHEMA, new java.util.Date(MILLIS_01_02_03_456)))
                .isEqualTo(expectedValue);
    }

    @Test
    void shouldConvertTimestampsAsIso8601WithTheirNativePrecision() {
        final ElasticsearchSinkConnectorConfig config = config();
        assertThat(convertField(config, Timestamp.schema(), 1_704_067_200_123L)).isEqualTo("2024-01-01T00:00:00.123Z");
        assertThat(convertField(config, MicroTimestamp.schema(), 1_704_067_200_123_456L)).isEqualTo("2024-01-01T00:00:00.123456Z");
        assertThat(convertField(config, NanoTimestamp.schema(), 1_704_067_200_123_456_789L)).isEqualTo("2024-01-01T00:00:00.123456789Z");
        assertThat(convertField(config, org.apache.kafka.connect.data.Timestamp.SCHEMA, new java.util.Date(MILLIS_2024_01_01)))
                .isEqualTo("2024-01-01T00:00:00Z");
    }

    @Test
    void shouldConvertTimestampsAsEpochNumbers() {
        final ElasticsearchSinkConnectorConfig millis = config(ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, "epoch_millis");
        assertThat(convertField(millis, NanoTimestamp.schema(), 1_704_067_200_123_456_789L)).isEqualTo(1_704_067_200_123L);
        assertThat(convertField(millis, Timestamp.schema(), 1_704_067_200_123L)).isEqualTo(1_704_067_200_123L);

        final ElasticsearchSinkConnectorConfig nanos = config(ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, "epoch_nanos");
        assertThat(convertField(nanos, Timestamp.schema(), 1_704_067_200_123L)).isEqualTo(1_704_067_200_123_000_000L);
        assertThat(convertField(nanos, MicroTimestamp.schema(), 1_704_067_200_123_456L)).isEqualTo(1_704_067_200_123_456_000L);
    }

    @Test
    void shouldPassZonedTemporalsThroughAndConvertYearToInteger() {
        assertThat(convertField(config(), ZonedTimestamp.schema(), "2024-01-01T00:00:00+02:00")).isEqualTo("2024-01-01T00:00:00+02:00");
        assertThat(convertField(config(), ZonedTime.schema(), "01:02:03+02:00")).isEqualTo("01:02:03+02:00");
        assertThat(convertField(config(), Year.schema(), 2024)).isEqualTo(2024);
    }

    @ParameterizedTest
    @CsvSource({ "string,PT1H0.5S", "numeric,3600500000.0" })
    void shouldConvertMicroDurationPerIntervalMode(String mode, String expected) {
        final Object converted = convertField(config(ElasticsearchSinkConnectorConfig.INTERVAL_OUTPUT_MODE, mode),
                MicroDuration.schema(), 3_600_500_000L);
        assertThat(converted).isEqualTo(mode.equals("string") ? expected : Double.parseDouble(expected));
    }

    @Test
    void shouldPassIntervalThroughAsIsoString() {
        // DDD-61 6.1 lists Interval under interval.output.mode; the code passes the ISO string through
        // in both modes, since a month-bearing interval has no single numeric micros value.
        final String iso = Interval.toIsoString(1, 2, 3, 4, 5, new BigDecimal("6.5"));
        assertThat(convertField(config(), Interval.schema(), iso)).isEqualTo(iso);
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.INTERVAL_OUTPUT_MODE, "numeric"), Interval.schema(), iso))
                .isEqualTo(iso);
    }

    @Test
    void shouldEmbedJsonAsObjectByDefaultAndAsStringOnRequest() {
        assertThat(convertField(config(), Json.schema(), "{\"a\":1,\"b\":[true]}"))
                .isEqualTo(Map.of("a", 1, "b", List.of(true)));
        assertThat(convertField(config(), Json.schema(), "[1,2]")).isEqualTo(List.of(1, 2));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.JSON_OUTPUT_MODE, "string"), Json.schema(), "{\"a\":1}"))
                .isEqualTo("{\"a\":1}");
    }

    @Test
    void shouldFailRecordOnUnparseableJson() {
        assertThatThrownBy(() -> convertField(config(), Json.schema(), "{not json"))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("json.output.mode=string");
    }

    @Test
    void shouldConvertStringLikeLogicalTypes() {
        assertThat(convertField(config(), Xml.schema(), "<a/>")).isEqualTo("<a/>");
        assertThat(convertField(config(), Uuid.schema(), "3f2504e0-4f89-11d3-9a0c-0305e82c3301")).isEqualTo("3f2504e0-4f89-11d3-9a0c-0305e82c3301");
        assertThat(convertField(config(), Enum.schema("a,b"), "a")).isEqualTo("a");
        assertThat(convertField(config(), EnumSet.schema("a,b,c"), "a,c")).isEqualTo(List.of("a", "c"));
        assertThat(convertField(config(), SchemaBuilder.string().name("io.debezium.data.Ltree").build(), "a.b")).isEqualTo("a.b");
    }

    @Test
    void shouldConvertBitsPerOutputMode() {
        final byte[] bits = { 0x05, 0x01 };
        assertThat(convertField(config(), Bits.schema(9), bits)).isEqualTo(Base64.getEncoder().encodeToString(bits));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.BITS_OUTPUT_MODE, "boolean_array"), Bits.schema(9), bits))
                .isEqualTo(List.of(true, false, true, false, false, false, false, false, true));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.BITS_OUTPUT_MODE, "integer"), Bits.schema(9), bits))
                .isEqualTo(261L);
    }

    @Test
    void shouldRejectIntegerBitsWiderThan64() {
        assertThatThrownBy(() -> convertField(config(ElasticsearchSinkConnectorConfig.BITS_OUTPUT_MODE, "integer"),
                Bits.schema(65), new byte[9]))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("65 bits");
    }

    @Test
    void shouldConvertPointToGeoJson() {
        final Schema schema = Point.builder().build();
        assertThat(convertField(config(), schema, Point.createValue(schema, 1.5, 2.5)))
                .isEqualTo(Map.of("type", "Point", "coordinates", List.of(1.5, 2.5)));
    }

    @Test
    void shouldConvertGeometryAndGeographyToGeoJsonFromWkb() {
        final byte[] wkb = pointWkb(1.5, 2.5);
        final Map<String, Object> expected = Map.of("type", "Point", "coordinates", List.of(1.5, 2.5));
        final Schema geometry = Geometry.builder().build();
        assertThat(convertField(config(), geometry, Geometry.createValue(geometry, wkb, 4326))).isEqualTo(expected);
        final Schema geography = Geography.builder().build();
        assertThat(convertField(config(), geography, Geography.createValue(geography, wkb, 4326, null))).isEqualTo(expected);
    }

    @Test
    void shouldWriteRawWkbWithSridUnderWkbMode() {
        final byte[] wkb = pointWkb(1.5, 2.5);
        final Schema geometry = Geometry.builder().build();
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.GEOMETRY_OUTPUT_MODE, "wkb"), geometry,
                Geometry.createValue(geometry, wkb, 4326)))
                .isEqualTo(Map.of("wkb", Base64.getEncoder().encodeToString(wkb), "srid", 4326));
        final Schema point = Point.builder().build();
        final Object rawPoint = convertField(config(ElasticsearchSinkConnectorConfig.GEOMETRY_OUTPUT_MODE, "wkb"), point,
                Point.createValue(point, 1.5, 2.5));
        @SuppressWarnings("unchecked")
        final Map<String, Object> rawPointMap = (Map<String, Object>) rawPoint;
        assertThat(rawPointMap).containsOnlyKeys("wkb");
        assertThat(rawPointMap.get("wkb")).isNotNull();
    }

    @Test
    void shouldFailRecordOnUnconvertibleWkb() {
        final Schema geometry = Geometry.builder().build();
        assertThatThrownBy(() -> convertField(config(), geometry, Geometry.createValue(geometry, new byte[]{ 1, 9, 0, 0, 0 }, null)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("geometry.output.mode=wkb");
    }

    @Test
    void shouldConvertVectorsPerOutputMode() {
        final Schema floats = FloatVector.builder().build();
        final Schema doubles = DoubleVector.builder().build();
        assertThat(convertField(config(), floats, List.of(1.0f, 2.0f))).isEqualTo(List.of(1.0f, 2.0f));
        assertThat(convertField(config(), doubles, List.of(1.0, 2.0))).isEqualTo(List.of(1.0, 2.0));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.VECTOR_OUTPUT_MODE, "string"), floats, List.of(1.0f, 2.0f)))
                .isEqualTo("[1.0, 2.0]");

        final Schema sparse = SparseDoubleVector.builder().build();
        final Map<Short, Double> entries = new LinkedHashMap<>();
        entries.put((short) 1, 0.5);
        entries.put((short) 3, 0.25);
        final Struct value = new Struct(sparse).put(SparseDoubleVector.DIMENSIONS_FIELD, (short) 5).put(SparseDoubleVector.VECTOR_FIELD, entries);
        assertThat(convertField(config(), sparse, value)).isEqualTo(Map.of("1", 0.5, "3", 0.25));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.VECTOR_OUTPUT_MODE, "string"), sparse, value))
                .isEqualTo("{1=0.5, 3=0.25}");
    }

    @Test
    void shouldConvertBytesPerBinaryMode() {
        final byte[] bytes = { (byte) 0xCA, (byte) 0xFE };
        assertThat(convertField(config(), Schema.BYTES_SCHEMA, bytes)).isEqualTo("yv4=");
        assertThat(convertField(config(), Schema.BYTES_SCHEMA, ByteBuffer.wrap(bytes))).isEqualTo("yv4=");
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.BINARY_OUTPUT_MODE, "hex"), Schema.BYTES_SCHEMA, bytes))
                .isEqualTo("cafe");
    }

    @Test
    void shouldConvertNestedStructsAndArrays() {
        final Schema inner = SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).field("d", Date.schema()).build();
        final Schema schema = SchemaBuilder.struct()
                .field("inner", inner)
                .field("dates", SchemaBuilder.array(Date.schema()).build())
                .build();
        final Struct payload = new Struct(schema)
                .put("inner", new Struct(inner).put("x", 1).put("d", (int) DAY_2024_01_01))
                .put("dates", List.of((int) DAY_2024_01_01));
        assertThat(convert(config(), schema, payload))
                .containsEntry("inner", Map.of("x", 1, "d", "2024-01-01"))
                .containsEntry("dates", List.of("2024-01-01"));
    }

    @Test
    void shouldConvertMapsCompactOrAsEntries() {
        final Schema stringKeys = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        final Schema intKeys = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build();
        assertThat(convertField(config(), stringKeys, Map.of("a", 1))).isEqualTo(Map.of("a", 1));
        assertThat(convertField(config(ElasticsearchSinkConnectorConfig.MAP_OUTPUT_MODE, "entries"), stringKeys, Map.of("a", 1)))
                .isEqualTo(List.of(Map.of("key", "a", "value", 1)));
        assertThat(convertField(config(), intKeys, Map.of(1, "a"))).isEqualTo(List.of(Map.of("key", 1, "value", "a")));
    }

    @Test
    void shouldOmitNullsByDefaultAndWriteThemOnRequest() {
        final Schema inner = SchemaBuilder.struct().field("x", Schema.OPTIONAL_INT32_SCHEMA).build();
        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("inner", inner)
                .build();
        final Struct payload = new Struct(schema).put("inner", new Struct(inner));

        assertThat(convert(config(), schema, payload)).isEqualTo(Map.of("inner", Map.of()));

        final Map<String, Object> written = convert(config(ElasticsearchSinkConnectorConfig.NULL_VALUE_HANDLING, "write_null"), schema, payload);
        assertThat(written).containsKey("name").containsEntry("name", null);
        @SuppressWarnings("unchecked")
        final Map<String, Object> innerWritten = (Map<String, Object>) written.get("inner");
        assertThat(innerWritten).containsKey("x");
    }

    @ParameterizedTest
    @CsvSource({ "none,user.name,user.name", "elasticsearch,user.name,user_name", "avro,user.name-1,user_name_1",
            "avro_unicode,user.name,user_u2ename", "avro,1st,_st" })
    void shouldAdjustFieldNamesPerMode(String mode, String name, String expected) {
        final Schema schema = SchemaBuilder.struct().field(name, Schema.STRING_SCHEMA).build();
        assertThat(convert(config(ElasticsearchSinkConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, mode), schema,
                new Struct(schema).put(name, "v"))).containsOnlyKeys(expected);
    }

    @Test
    void shouldHonorCustomSeparatorReplacement() {
        final Schema schema = SchemaBuilder.struct().field("user.name", Schema.STRING_SCHEMA).build();
        final ElasticsearchSinkConnectorConfig config = config(ElasticsearchSinkConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, "elasticsearch",
                ElasticsearchSinkConnectorConfig.FIELD_NAME_SEPARATOR_REPLACEMENT, "__");
        assertThat(convert(config, schema, new Struct(schema).put("user.name", "v"))).containsOnlyKeys("user__name");
    }

    @Test
    void shouldApplyFieldIncludeAndExcludeListsBeforeConversion() {
        final Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("secret", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct payload = new Struct(schema).put("id", 1).put("secret", "s").put("name", "n");
        assertThat(convert(config("field.exclude.list", TOPIC + ":secret"), schema, payload)).containsOnlyKeys("id", "name");
        assertThat(convert(config("field.include.list", "id"), schema, payload)).containsOnlyKeys("id");
    }

    @Test
    void shouldProjectKafkaMetadataWithPrefix() {
        final Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        final Struct payload = new Struct(schema).put("name", "n");
        final SinkRecord sinkRecord = new SinkRecord(TOPIC, 3, KEY_SCHEMA, new Struct(KEY_SCHEMA).put("id", 9), schema, payload, 42L,
                1_700_000_000_000L, TimestampType.CREATE_TIME);
        final ElasticsearchSinkConnectorConfig config = config(
                ElasticsearchSinkConnectorConfig.DOCUMENT_METADATA_FIELDS, "topic,partition,offset,timestamp,key",
                ElasticsearchSinkConnectorConfig.DOCUMENT_METADATA_PREFIX, "_meta_");
        final Map<String, Object> document = new DocumentConverter(config)
                .convert(SinkOperation.Write.of(new ElasticsearchSinkRecord(sinkRecord, null), null, payload));
        assertThat(document)
                .containsEntry("name", "n")
                .containsEntry("_meta_topic", TOPIC)
                .containsEntry("_meta_partition", 3)
                .containsEntry("_meta_offset", 42L)
                .containsEntry("_meta_timestamp", 1_700_000_000_000L)
                .containsEntry("_meta_key", Map.of("id", 9));
    }

    @Test
    void shouldConvertSchemalessMapValuesAndRejectScalars() {
        final Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("a", 1);
        raw.put("nested", Map.of("b", List.of("x")));
        raw.put("bytes", new byte[]{ (byte) 0xCA, (byte) 0xFE });
        final SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, null, raw, 1L);
        final ElasticsearchSinkRecord record = new ElasticsearchSinkRecord(sinkRecord, null);
        assertThat(new DocumentConverter(config()).convert(SinkOperation.Write.ofRaw(record, raw)))
                .containsEntry("a", 1)
                .containsEntry("nested", Map.of("b", List.of("x")))
                .containsEntry("bytes", "yv4=");

        final ElasticsearchSinkRecord scalar = new ElasticsearchSinkRecord(new SinkRecord(TOPIC, 0, null, null, null, "text", 1L), null);
        assertThatThrownBy(() -> new DocumentConverter(config()).convert(SinkOperation.Write.ofRaw(scalar, "text")))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("String");
    }

    @Test
    void shouldResolveRoutingValueByDottedPathAndFailWhenAbsent() {
        final DocumentConverter converter = new DocumentConverter(config());
        final Map<String, Object> document = Map.of("tenant", Map.of("id", 7));
        final ElasticsearchSinkRecord record = new ElasticsearchSinkRecord(new SinkRecord(TOPIC, 0, null, null, null, null, 1L), null);
        assertThat(converter.routingValue(record, document, "tenant.id")).isEqualTo("7");
        assertThatThrownBy(() -> converter.routingValue(record, document, "tenant.region"))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("tenant.region");
    }

    private static ElasticsearchSinkConnectorConfig config(String... keyValues) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "http://localhost:9200");
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return new ElasticsearchSinkConnectorConfig(properties);
    }

    private static Object convertField(ElasticsearchSinkConnectorConfig config, Schema fieldSchema, Object value) {
        final Schema schema = SchemaBuilder.struct().name("Value").field("f", fieldSchema).build();
        return convert(config, schema, new Struct(schema).put("f", value)).get("f");
    }

    private static Map<String, Object> convert(ElasticsearchSinkConnectorConfig config, Schema schema, Struct payload) {
        final SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, KEY_SCHEMA, new Struct(KEY_SCHEMA).put("id", 1), schema, payload, 7L);
        final ElasticsearchSinkRecord record = new ElasticsearchSinkRecord(sinkRecord, null);
        return new DocumentConverter(config).convert(SinkOperation.Write.of(record, null, payload));
    }

    private static byte[] pointWkb(double x, double y) {
        return ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN).put((byte) 1).putInt(1).putDouble(x).putDouble(y).array();
    }
}
