/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
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

import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;

/**
 * The mapping half of the type-fidelity matrix: the Elasticsearch field type
 * {@link MappingGenerator} derives for each Connect and Debezium type under each output mode,
 * per DDD-61 6.3.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class MappingGeneratorTest {

    private static final String TEMPORAL = ElasticsearchSinkConnectorConfig.TEMPORAL_OUTPUT_MODE;
    private static final String DECIMAL = ElasticsearchSinkConnectorConfig.DECIMAL_OUTPUT_MODE;

    @ParameterizedTest
    @CsvSource({ "INT8,Short", "INT16,Short", "INT32,Integer", "INT64,Long", "FLOAT32,Float", "FLOAT64,Double",
            "BOOLEAN,Boolean", "BYTES,Binary" })
    void shouldMapPrimitives(Schema.Type type, String kind) {
        assertThat(property(config(), SchemaBuilder.type(type).build())._kind()).isEqualTo(Property.Kind.valueOf(kind));
    }

    @Test
    void shouldMapStringsPerStringMappingMode() {
        final Property withKeyword = property(config(), Schema.STRING_SCHEMA);
        assertThat(withKeyword._kind()).isEqualTo(Property.Kind.Text);
        assertThat(withKeyword.text().fields()).containsKey("keyword");
        assertThat(withKeyword.text().fields().get("keyword").keyword().ignoreAbove()).isEqualTo(256);

        assertThat(property(config(ElasticsearchSinkConnectorConfig.STRING_MAPPING_MODE, "text"), Schema.STRING_SCHEMA)._kind())
                .isEqualTo(Property.Kind.Text);
        assertThat(property(config(ElasticsearchSinkConnectorConfig.STRING_MAPPING_MODE, "keyword"), Schema.STRING_SCHEMA)._kind())
                .isEqualTo(Property.Kind.Keyword);
    }

    @Test
    void shouldMapDecimalsPerOutputMode() {
        final Property scaled = property(config(), Decimal.schema(2));
        assertThat(scaled._kind()).isEqualTo(Property.Kind.ScaledFloat);
        assertThat(scaled.scaledFloat().scalingFactor()).isEqualTo(100.0);
        // DDD-61 6.3 says scaled_float with the schema scale; the code maps a zero-scale decimal as long.
        assertThat(property(config(), Decimal.schema(0))._kind()).isEqualTo(Property.Kind.Long);
        assertThat(property(config(DECIMAL, "string"), Decimal.schema(2))._kind()).isEqualTo(Property.Kind.Keyword);
        assertThat(property(config(DECIMAL, "double"), Decimal.schema(2))._kind()).isEqualTo(Property.Kind.Double);

        assertThat(property(config(), VariableScaleDecimal.schema())._kind()).isEqualTo(Property.Kind.Double);
        assertThat(property(config(DECIMAL, "string"), VariableScaleDecimal.schema())._kind()).isEqualTo(Property.Kind.Keyword);
    }

    @ParameterizedTest
    @CsvSource({ "iso8601,Date,strict_date||epoch_millis", "epoch_millis,Date,epoch_millis", "epoch_nanos,Long," })
    void shouldMapDatesPerTemporalMode(String mode, String kind, String format) {
        for (Schema schema : new Schema[]{ Date.schema(), org.apache.kafka.connect.data.Date.SCHEMA }) {
            final Property property = property(config(TEMPORAL, mode), schema);
            assertThat(property._kind()).isEqualTo(Property.Kind.valueOf(kind));
            if (format != null) {
                assertThat(property.date().format()).isEqualTo(format);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "iso8601,Keyword", "epoch_millis,Long", "epoch_nanos,Long" })
    void shouldMapTimesPerTemporalMode(String mode, String kind) {
        // DDD-61 6.3 says temporal types map as date; a time of day has no date type, so the
        // code maps the ISO string as keyword and the epoch numbers as long.
        for (Schema schema : new Schema[]{ Time.schema(), MicroTime.schema(), NanoTime.schema(), org.apache.kafka.connect.data.Time.SCHEMA }) {
            assertThat(property(config(TEMPORAL, mode), schema)._kind()).isEqualTo(Property.Kind.valueOf(kind));
        }
    }

    @ParameterizedTest
    @CsvSource({ "iso8601,Date,strict_date_optional_time||epoch_millis", "epoch_millis,Date,epoch_millis", "epoch_nanos,Long," })
    void shouldMapMillisAndMicrosTimestampsPerTemporalMode(String mode, String kind, String format) {
        for (Schema schema : new Schema[]{ Timestamp.schema(), MicroTimestamp.schema(), org.apache.kafka.connect.data.Timestamp.SCHEMA }) {
            final Property property = property(config(TEMPORAL, mode), schema);
            assertThat(property._kind()).isEqualTo(Property.Kind.valueOf(kind));
            if (format != null) {
                assertThat(property.date().format()).isEqualTo(format);
            }
        }
    }

    @Test
    void shouldMapNanoTimestampWithNanosecondPrecision() {
        final Property nanos = property(config(), NanoTimestamp.schema());
        assertThat(nanos._kind()).isEqualTo(Property.Kind.DateNanos);
        assertThat(nanos.dateNanos().format()).isEqualTo("strict_date_optional_time_nanos");
        assertThat(property(config(TEMPORAL, "epoch_millis"), NanoTimestamp.schema()).date().format()).isEqualTo("epoch_millis");
        assertThat(property(config(TEMPORAL, "epoch_nanos"), NanoTimestamp.schema())._kind()).isEqualTo(Property.Kind.Long);
    }

    @ParameterizedTest
    @CsvSource({ "iso8601", "epoch_millis", "epoch_nanos" })
    void shouldMapZonedTimestampAsDateParsingIsoStringsInEveryMode(String mode) {
        // The converter passes ZonedTimestamp through as an ISO string regardless of
        // temporal.output.mode (DDD-61 6.1), so the mapping must parse ISO strings in every mode;
        // the code maps it like an epoch timestamp instead, which cannot index the written value.
        final Property property = property(config(TEMPORAL, mode), ZonedTimestamp.schema());
        assertThat(property._kind()).isEqualTo(Property.Kind.Date);
        assertThat(property.date().format()).contains("strict_date_optional_time");
    }

    @Test
    void shouldMapRemainingTemporalsAndIntervals() {
        assertThat(property(config(), ZonedTime.schema())._kind()).isEqualTo(Property.Kind.Keyword);
        assertThat(property(config(), Year.schema())._kind()).isEqualTo(Property.Kind.Integer);
        assertThat(property(config(), Interval.schema())._kind()).isEqualTo(Property.Kind.Keyword);
        assertThat(property(config(), MicroDuration.schema())._kind()).isEqualTo(Property.Kind.Keyword);
        assertThat(property(config(ElasticsearchSinkConnectorConfig.INTERVAL_OUTPUT_MODE, "numeric"), MicroDuration.schema())._kind())
                .isEqualTo(Property.Kind.Double);
    }

    @Test
    void shouldMapJsonPerOutputAndMappingMode() {
        final Property object = property(config(), Json.schema());
        assertThat(object._kind()).isEqualTo(Property.Kind.Object);
        assertThat(object.object().dynamic()).isEqualTo(DynamicMapping.True);
        assertThat(property(config(ElasticsearchSinkConnectorConfig.JSON_MAPPING_MODE, "flattened"), Json.schema())._kind())
                .isEqualTo(Property.Kind.Flattened);
        assertThat(property(config(ElasticsearchSinkConnectorConfig.JSON_OUTPUT_MODE, "string"), Json.schema())._kind())
                .isEqualTo(Property.Kind.Text);
    }

    @Test
    void shouldMapStringLikeLogicalTypesAsKeyword() {
        for (Schema schema : new Schema[]{ Xml.schema(), Uuid.schema(), Enum.schema("a,b"), EnumSet.schema("a,b"),
                SchemaBuilder.string().name("io.debezium.data.Ltree").build() }) {
            assertThat(property(config(), schema)._kind()).as(schema.name()).isEqualTo(Property.Kind.Keyword);
        }
    }

    @ParameterizedTest
    @CsvSource({ "base64,Binary", "boolean_array,Boolean", "integer,Long" })
    void shouldMapBitsPerOutputMode(String mode, String kind) {
        assertThat(property(config(ElasticsearchSinkConnectorConfig.BITS_OUTPUT_MODE, mode), Bits.schema(9))._kind())
                .isEqualTo(Property.Kind.valueOf(kind));
    }

    @Test
    void shouldMapGeometryPerOutputMode() {
        assertThat(property(config(), Point.builder().build())._kind()).isEqualTo(Property.Kind.GeoPoint);
        assertThat(property(config(), Geometry.builder().build())._kind()).isEqualTo(Property.Kind.GeoShape);
        assertThat(property(config(), Geography.builder().build())._kind()).isEqualTo(Property.Kind.GeoShape);
        final Property wkb = property(config(ElasticsearchSinkConnectorConfig.GEOMETRY_OUTPUT_MODE, "wkb"), Geometry.builder().build());
        assertThat(wkb._kind()).isEqualTo(Property.Kind.Object);
        assertThat(wkb.object().dynamic()).isEqualTo(DynamicMapping.True);
    }

    @Test
    void shouldMapVectorsPerOutputMode() {
        assertThat(property(config(), FloatVector.builder().build())._kind()).isEqualTo(Property.Kind.DenseVector);
        assertThat(property(config(), DoubleVector.builder().build())._kind()).isEqualTo(Property.Kind.DenseVector);
        assertThat(property(config(), SparseDoubleVector.builder().build())._kind()).isEqualTo(Property.Kind.SparseVector);
        final ElasticsearchSinkConnectorConfig string = config(ElasticsearchSinkConnectorConfig.VECTOR_OUTPUT_MODE, "string");
        assertThat(property(string, FloatVector.builder().build())._kind()).isEqualTo(Property.Kind.Keyword);
        assertThat(property(string, SparseDoubleVector.builder().build())._kind()).isEqualTo(Property.Kind.Keyword);
    }

    @Test
    void shouldMapStructsPerStructMappingModeWithNestedProperties() {
        final Schema struct = SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).field("when", Date.schema()).build();
        final Property object = property(config(), struct);
        assertThat(object._kind()).isEqualTo(Property.Kind.Object);
        assertThat(object.object().properties()).containsOnlyKeys("x", "when");
        assertThat(object.object().properties().get("when")._kind()).isEqualTo(Property.Kind.Date);

        final Property nested = property(config(ElasticsearchSinkConnectorConfig.STRUCT_MAPPING_MODE, "nested"), struct);
        assertThat(nested._kind()).isEqualTo(Property.Kind.Nested);
        assertThat(nested.nested().properties()).containsOnlyKeys("x", "when");
    }

    @Test
    void shouldMapArraysByElementTypeAndMapsAsDynamicObjects() {
        assertThat(property(config(), SchemaBuilder.array(Date.schema()).build())._kind()).isEqualTo(Property.Kind.Date);
        final Property map = property(config(), SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build());
        assertThat(map._kind()).isEqualTo(Property.Kind.Object);
        assertThat(map.object().dynamic()).isEqualTo(DynamicMapping.True);
    }

    @ParameterizedTest
    @CsvSource({ "strict_but_dlq,Strict", "true,True", "false,False", "runtime,Runtime" })
    void shouldSetDynamicPerMappingDynamic(String mode, String expected) {
        final TypeMapping mapping = generate(config(ElasticsearchSinkConnectorConfig.MAPPING_DYNAMIC, mode), Schema.STRING_SCHEMA);
        assertThat(mapping.dynamic()).isEqualTo(DynamicMapping.valueOf(expected));
    }

    @Test
    void shouldPreserveFieldOrder() {
        final Schema schema = SchemaBuilder.struct().field("b", Schema.STRING_SCHEMA).field("a", Schema.INT32_SCHEMA).build();
        assertThat(new MappingGenerator(config()).generate(schema).properties().keySet()).containsExactly("b", "a");
    }

    private static ElasticsearchSinkConnectorConfig config(String... keyValues) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "http://localhost:9200");
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return new ElasticsearchSinkConnectorConfig(properties);
    }

    private static TypeMapping generate(ElasticsearchSinkConnectorConfig config, Schema fieldSchema) {
        return new MappingGenerator(config).generate(SchemaBuilder.struct().field("f", fieldSchema).build());
    }

    private static Property property(ElasticsearchSinkConnectorConfig config, Schema fieldSchema) {
        return generate(config, fieldSchema).properties().get("f");
    }
}
