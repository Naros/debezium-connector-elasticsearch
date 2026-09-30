/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.elasticsearch.junit.ElasticsearchTestCluster;
import io.debezium.connector.elasticsearch.junit.jupiter.ElasticsearchExtension;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Envelope;
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

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.GetResponse;

/**
 * Round-trips every logical type the converter handles through a live cluster and asserts that
 * the generated mapping and the written document agree: the index is created from the mapping,
 * the document indexes without reaching the error reporter, and each field carries the expected
 * Elasticsearch type (DDD-61 Testing, "Type fidelity").
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@ExtendWith(ElasticsearchExtension.class)
public class ElasticsearchSinkTypeFidelityIT {

    private static final String DEFAULT_TOPIC = "fidelity.default";
    private static final List<String> TOPICS = List.of(DEFAULT_TOPIC, "fidelity.epoch-millis", "fidelity.epoch-nanos",
            "fidelity.decimal-string", "fidelity.geometry-wkb");

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("Source")
            .field("ts_ms", Schema.INT64_SCHEMA).field("db", Schema.STRING_SCHEMA).field("table", Schema.STRING_SCHEMA).build();
    private static final Schema INNER_SCHEMA = SchemaBuilder.struct().name("Inner")
            .field("x", Schema.INT32_SCHEMA).field("when", Date.schema()).build();
    private static final Schema SPARSE_SCHEMA = SparseDoubleVector.builder().build();

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("decimal", Decimal.schema(2))
            .field("decimal0", Decimal.schema(0))
            .field("vardec", VariableScaleDecimal.schema())
            .field("date", Date.schema())
            .field("time", Time.schema())
            .field("microtime", MicroTime.schema())
            .field("nanotime", NanoTime.schema())
            .field("ts", Timestamp.schema())
            .field("microts", MicroTimestamp.schema())
            .field("nanots", NanoTimestamp.schema())
            .field("zonedts", ZonedTimestamp.schema())
            .field("zonedtime", ZonedTime.schema())
            .field("year", Year.schema())
            .field("interval", Interval.schema())
            .field("duration", MicroDuration.schema())
            .field("json", Json.schema())
            .field("xml", Xml.schema())
            .field("uuid", Uuid.schema())
            .field("enum", Enum.schema("a,b"))
            .field("enumset", EnumSet.schema("a,b,c"))
            .field("ltree", SchemaBuilder.string().name("io.debezium.data.Ltree").build())
            .field("bits", Bits.schema(9))
            .field("point", Point.builder().build())
            .field("geometry", Geometry.builder().build())
            .field("geography", Geography.builder().build())
            .field("floats", FloatVector.builder().build())
            .field("doubles", DoubleVector.builder().build())
            .field("sparse", SPARSE_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("text", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("inner", INNER_SCHEMA)
            .field("ints", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();

    private ElasticsearchTestCluster cluster;
    private ElasticsearchSinkConnectorTask task;
    private ElasticsearchSinkTaskTestContext context;

    @BeforeEach
    void beforeEach(ElasticsearchTestCluster cluster) throws IOException {
        this.cluster = cluster;
        cluster.reset(TOPICS);
    }

    @AfterEach
    void afterEach() {
        if (task != null) {
            task.stop();
        }
    }

    @Test
    void shouldIndexEveryLogicalTypeUnderTheGeneratedMapping() throws IOException {
        startTask(Map.of());

        task.put(List.of(record(DEFAULT_TOPIC)));
        task.preCommit(Map.of(new TopicPartition(DEFAULT_TOPIC, 0), new OffsetAndMetadata(0)));

        assertThat(context.reportedRecords()).as("every field indexed under the generated mapping").isEmpty();
        final Map<String, Object> document = document(DEFAULT_TOPIC);
        assertThat(document).containsEntry("decimal", 12.34).containsEntry("decimal0", 5).containsEntry("vardec", 1.5)
                .containsEntry("date", "2024-01-01").containsEntry("ts", "2024-01-01T00:00:00.123Z")
                .containsEntry("nanots", "2024-01-01T00:00:00.123456789Z").containsEntry("zonedts", "2024-01-01T00:00:00+02:00")
                .containsEntry("year", 2024).containsEntry("json", Map.of("a", 1)).containsEntry("enumset", List.of("a", "c"))
                .containsEntry("point", Map.of("type", "Point", "coordinates", List.of(1.5, 2.5)))
                .containsEntry("inner", Map.of("x", 7, "when", "2024-01-01")).containsEntry("ints", List.of(1, 2))
                .containsEntry("map", Map.of("k", 1));
        assertThat(document).containsKeys("time", "microtime", "nanotime", "microts", "zonedtime", "interval", "duration", "xml",
                "uuid", "enum", "ltree", "bits", "geometry", "geography", "int8", "int16", "int64", "float32", "float64",
                "bool", "text", "bytes");
        // Elasticsearch 9 excludes vector fields from _source by default, so the vectors are
        // asserted only when the cluster returns them; the mapping assertion below covers both.
        if (document.containsKey("floats")) {
            assertThat(document).containsEntry("floats", List.of(1.0, 2.0)).containsKey("doubles")
                    .containsEntry("sparse", Map.of("1", 0.5, "3", 0.25));
        }

        final Map<String, Property> mapping = mapping(DEFAULT_TOPIC);
        assertKinds(mapping, Map.ofEntries(
                Map.entry("decimal", Property.Kind.ScaledFloat), Map.entry("decimal0", Property.Kind.Long),
                Map.entry("vardec", Property.Kind.Double), Map.entry("date", Property.Kind.Date),
                Map.entry("time", Property.Kind.Keyword), Map.entry("microtime", Property.Kind.Keyword),
                Map.entry("nanotime", Property.Kind.Keyword), Map.entry("ts", Property.Kind.Date),
                Map.entry("microts", Property.Kind.Date), Map.entry("nanots", Property.Kind.DateNanos),
                Map.entry("zonedts", Property.Kind.Date), Map.entry("zonedtime", Property.Kind.Keyword),
                Map.entry("year", Property.Kind.Integer), Map.entry("interval", Property.Kind.Keyword),
                Map.entry("duration", Property.Kind.Keyword), Map.entry("json", Property.Kind.Object),
                Map.entry("xml", Property.Kind.Keyword), Map.entry("uuid", Property.Kind.Keyword),
                Map.entry("enum", Property.Kind.Keyword), Map.entry("enumset", Property.Kind.Keyword),
                Map.entry("ltree", Property.Kind.Keyword), Map.entry("bits", Property.Kind.Binary),
                Map.entry("point", Property.Kind.GeoPoint), Map.entry("geometry", Property.Kind.GeoShape),
                Map.entry("geography", Property.Kind.GeoShape), Map.entry("floats", Property.Kind.DenseVector),
                Map.entry("doubles", Property.Kind.DenseVector), Map.entry("sparse", Property.Kind.SparseVector),
                Map.entry("int8", Property.Kind.Short), Map.entry("int16", Property.Kind.Short),
                Map.entry("id", Property.Kind.Integer), Map.entry("int64", Property.Kind.Long),
                Map.entry("float32", Property.Kind.Float), Map.entry("float64", Property.Kind.Double),
                Map.entry("bool", Property.Kind.Boolean), Map.entry("text", Property.Kind.Text),
                Map.entry("bytes", Property.Kind.Binary), Map.entry("inner", Property.Kind.Object),
                Map.entry("ints", Property.Kind.Integer), Map.entry("map", Property.Kind.Object)));
        assertThat(mapping.get("decimal").scaledFloat().scalingFactor()).isEqualTo(100.0);
        assertThat(mapping.get("inner").object().properties().get("when")._kind()).isEqualTo(Property.Kind.Date);
    }

    static Stream<Arguments> outputModes() {
        return Stream.of(
                Arguments.of("fidelity.epoch-millis", ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, "epoch_millis"),
                Arguments.of("fidelity.epoch-nanos", ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE, "epoch_nanos"),
                Arguments.of("fidelity.decimal-string", ElasticsearchSinkConnectorConfig.DECIMAL_OUTPUT_MODE, "string"),
                Arguments.of("fidelity.geometry-wkb", ElasticsearchSinkConnectorConfig.GEOMETRY_OUTPUT_MODE, "wkb"));
    }

    @ParameterizedTest(name = "{1}={2}")
    @MethodSource("outputModes")
    void shouldKeepMappingAndDocumentInAgreementUnderEachOutputMode(String topic, String property, String mode) throws IOException {
        startTask(Map.of(property, mode));

        task.put(List.of(record(topic)));
        task.preCommit(Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(0)));

        assertThat(context.reportedRecords()).as("the document written under %s=%s indexes under its own mapping", property, mode)
                .isEmpty();
        final Map<String, Object> document = document(topic);
        assertThat(document).containsKey("ts").containsKey("nanots").containsKey("zonedts").containsKey("decimal").containsKey("geometry");
        final Map<String, Property> mapping = mapping(topic);
        switch (mode) {
            case "epoch_millis" -> {
                assertThat(mapping.get("ts").date().format()).isEqualTo("epoch_millis");
                assertThat(document).containsEntry("ts", 1_704_067_200_123L).containsEntry("zonedts", "2024-01-01T00:00:00+02:00");
            }
            case "epoch_nanos" -> {
                assertThat(mapping.get("ts")._kind()).isEqualTo(Property.Kind.Long);
                assertThat(mapping.get("nanots")._kind()).isEqualTo(Property.Kind.Long);
                assertThat(mapping.get("zonedts")._kind()).isEqualTo(Property.Kind.Date);
            }
            case "string" -> {
                assertThat(mapping.get("decimal")._kind()).isEqualTo(Property.Kind.Keyword);
                assertThat(document).containsEntry("decimal", "12.34");
            }
            case "wkb" -> {
                assertThat(mapping.get("geometry")._kind()).isEqualTo(Property.Kind.Object);
                assertThat(document.get("geometry")).isInstanceOf(Map.class);
            }
            default -> throw new IllegalArgumentException(mode);
        }
    }

    private void startTask(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>(cluster.connectionProperties());
        properties.put("name", "fidelity");
        properties.put("errors.tolerance", "all");
        properties.putAll(overrides);

        context = new ElasticsearchSinkTaskTestContext(properties);
        task = new ElasticsearchSinkConnectorTask();
        task.initialize(context);
        task.start(properties);
        task.open(TOPICS.stream().map(t -> new TopicPartition(t, 0)).toList());
    }

    private static SinkRecord record(String topic) {
        final Map<Short, Double> sparse = new LinkedHashMap<>();
        sparse.put((short) 1, 0.5);
        sparse.put((short) 3, 0.25);
        final byte[] wkb = pointWkb(1.5, 2.5);
        final Schema geometry = VALUE_SCHEMA.field("geometry").schema();
        final Schema geography = VALUE_SCHEMA.field("geography").schema();
        final Schema point = VALUE_SCHEMA.field("point").schema();
        final Struct value = new Struct(VALUE_SCHEMA)
                .put("id", 1)
                .put("decimal", new BigDecimal("12.34"))
                .put("decimal0", new BigDecimal("5"))
                .put("vardec", VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), new BigDecimal("1.5")))
                .put("date", 19723)
                .put("time", 3_723_456)
                .put("microtime", 3_723_456_000L)
                .put("nanotime", 3_723_456_000_000L)
                .put("ts", 1_704_067_200_123L)
                .put("microts", 1_704_067_200_123_456L)
                .put("nanots", 1_704_067_200_123_456_789L)
                .put("zonedts", "2024-01-01T00:00:00+02:00")
                .put("zonedtime", "01:02:03+02:00")
                .put("year", 2024)
                .put("interval", Interval.toIsoString(1, 2, 3, 4, 5, new BigDecimal("6.5")))
                .put("duration", 3_600_500_000L)
                .put("json", "{\"a\":1}")
                .put("xml", "<a/>")
                .put("uuid", "3f2504e0-4f89-11d3-9a0c-0305e82c3301")
                .put("enum", "a")
                .put("enumset", "a,c")
                .put("ltree", "a.b")
                .put("bits", new byte[]{ 0x05, 0x01 })
                .put("point", Point.createValue(point, 1.5, 2.5))
                .put("geometry", Geometry.createValue(geometry, wkb, 4326))
                .put("geography", Geography.createValue(geography, wkb, 4326, null))
                .put("floats", List.of(1.0f, 2.0f))
                .put("doubles", List.of(1.0, 2.0))
                .put("sparse", new Struct(SPARSE_SCHEMA).put(SparseDoubleVector.DIMENSIONS_FIELD, (short) 5)
                        .put(SparseDoubleVector.VECTOR_FIELD, sparse))
                .put("int8", (byte) 1)
                .put("int16", (short) 2)
                .put("int64", 3L)
                .put("float32", 1.5f)
                .put("float64", 2.5)
                .put("bool", true)
                .put("text", "hello")
                .put("bytes", new byte[]{ (byte) 0xCA, (byte) 0xFE })
                .put("inner", new Struct(INNER_SCHEMA).put("x", 7).put("when", 19723))
                .put("ints", List.of(1, 2))
                .put("map", Map.of("k", 1));

        final Envelope envelope = Envelope.defineSchema().withName(topic + ".Envelope").withRecord(VALUE_SCHEMA).withSource(SOURCE_SCHEMA).build();
        final Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", Instant.now().toEpochMilli()).put("db", "inventory").put("table", "types");
        final Struct key = new Struct(KEY_SCHEMA).put("id", 1);
        return new SinkRecord(topic, 0, KEY_SCHEMA, key, envelope.schema(), envelope.create(value, source, Instant.now()), 0);
    }

    private static byte[] pointWkb(double x, double y) {
        return ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN).put((byte) 1).putInt(1).putDouble(x).putDouble(y).array();
    }

    private void assertKinds(Map<String, Property> mapping, Map<String, Property.Kind> expected) {
        expected.forEach((field, kind) -> {
            assertThat(mapping).as("field %s is mapped", field).containsKey(field);
            assertThat(mapping.get(field)._kind()).as("field %s", field).isEqualTo(kind);
        });
    }

    private Map<String, Property> mapping(String index) throws IOException {
        return cluster.client().indices().getMapping(g -> g.index(index)).result().get(index).mappings().properties();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> document(String index) throws IOException {
        final GetResponse<Map> response = cluster.client().get(g -> g.index(index).id("1"), Map.class);
        assertThat(response.found()).as("document 1 in %s", index).isTrue();
        return response.source();
    }
}
