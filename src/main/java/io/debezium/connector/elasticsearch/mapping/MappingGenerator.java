/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.mapping;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.GeometryOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.IntervalOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.JsonMappingMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.JsonOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.StructMappingMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TemporalOutputMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.VectorOutputMode;
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
 * Derives an Elasticsearch {@link TypeMapping} from a Connect schema, honoring the configured
 * output modes so that the mapping matches what the {@code DocumentConverter} actually writes.
 * Under {@code temporal.output.mode=epoch_nanos} temporals map as {@code long}, since
 * Elasticsearch's date types do not parse raw nanosecond numbers.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 6.3"
 */
public class MappingGenerator {

    private final ElasticsearchSinkConnectorConfig config;

    public MappingGenerator(ElasticsearchSinkConnectorConfig config) {
        this.config = config;
    }

    public TypeMapping generate(Schema valueSchema) {
        final Map<String, Property> properties = new LinkedHashMap<>();
        for (Field field : valueSchema.fields()) {
            properties.put(field.name(), property(field.schema()));
        }
        return TypeMapping.of(mapping -> mapping
                .dynamic(dynamicMapping())
                .properties(properties));
    }

    private DynamicMapping dynamicMapping() {
        return switch (config.mappingDynamic()) {
            case STRICT_BUT_DLQ -> DynamicMapping.Strict;
            case TRUE -> DynamicMapping.True;
            case FALSE -> DynamicMapping.False;
            case RUNTIME -> DynamicMapping.Runtime;
        };
    }

    private Property property(Schema schema) {
        if (schema.name() != null) {
            final Property logical = logicalProperty(schema);
            if (logical != null) {
                return logical;
            }
        }
        return switch (schema.type()) {
            case INT8, INT16 -> Property.of(p -> p.short_(s -> s));
            case INT32 -> Property.of(p -> p.integer(i -> i));
            case INT64 -> Property.of(p -> p.long_(l -> l));
            case FLOAT32 -> Property.of(p -> p.float_(f -> f));
            case FLOAT64 -> Property.of(p -> p.double_(d -> d));
            case BOOLEAN -> Property.of(p -> p.boolean_(b -> b));
            case STRING -> stringProperty();
            case BYTES -> Property.of(p -> p.binary(b -> b));
            case STRUCT -> structProperty(schema);
            case ARRAY -> property(schema.valueSchema());
            case MAP -> dynamicObject();
        };
    }

    private Property logicalProperty(Schema schema) {
        return switch (schema.name()) {
            case Decimal.LOGICAL_NAME -> decimalProperty(schema);
            case VariableScaleDecimal.LOGICAL_NAME -> switch (config.decimalOutputMode()) {
                case STRING -> keyword();
                default -> Property.of(p -> p.double_(d -> d));
            };
            case Date.SCHEMA_NAME, org.apache.kafka.connect.data.Date.LOGICAL_NAME ->
                dateProperty("strict_date||epoch_millis");
            case Time.SCHEMA_NAME, MicroTime.SCHEMA_NAME, NanoTime.SCHEMA_NAME,
                    org.apache.kafka.connect.data.Time.LOGICAL_NAME ->
                config.temporalOutputMode() == TemporalOutputMode.ISO8601 ? keyword() : Property.of(p -> p.long_(l -> l));
            case Timestamp.SCHEMA_NAME, MicroTimestamp.SCHEMA_NAME,
                    org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, ZonedTimestamp.SCHEMA_NAME ->
                dateProperty("strict_date_optional_time||epoch_millis");
            case NanoTimestamp.SCHEMA_NAME -> switch (config.temporalOutputMode()) {
                case ISO8601 -> Property.of(p -> p.dateNanos(d -> d.format("strict_date_optional_time_nanos")));
                case EPOCH_MILLIS -> dateProperty("epoch_millis");
                case EPOCH_NANOS -> Property.of(p -> p.long_(l -> l));
            };
            case ZonedTime.SCHEMA_NAME -> keyword();
            case Year.SCHEMA_NAME -> Property.of(p -> p.integer(i -> i));
            case Interval.SCHEMA_NAME -> keyword();
            case MicroDuration.SCHEMA_NAME ->
                config.intervalOutputMode() == IntervalOutputMode.NUMERIC
                        ? Property.of(p -> p.double_(d -> d))
                        : keyword();
            case Json.LOGICAL_NAME -> jsonProperty();
            // "io.debezium.data.Ltree" is defined by the Postgres connector, referenced by name.
            case Xml.LOGICAL_NAME, Uuid.LOGICAL_NAME, Enum.LOGICAL_NAME, EnumSet.LOGICAL_NAME,
                    "io.debezium.data.Ltree" ->
                keyword();
            case Bits.LOGICAL_NAME -> switch (config.bitsOutputMode()) {
                case BASE64 -> Property.of(p -> p.binary(b -> b));
                case BOOLEAN_ARRAY -> Property.of(p -> p.boolean_(b -> b));
                case INTEGER -> Property.of(p -> p.long_(l -> l));
            };
            case Point.LOGICAL_NAME -> geometryProperty(true);
            case Geometry.LOGICAL_NAME, Geography.LOGICAL_NAME -> geometryProperty(false);
            case FloatVector.LOGICAL_NAME, DoubleVector.LOGICAL_NAME ->
                config.vectorOutputMode() == VectorOutputMode.STRING
                        ? keyword()
                        : Property.of(p -> p.denseVector(d -> d));
            case SparseDoubleVector.LOGICAL_NAME ->
                config.vectorOutputMode() == VectorOutputMode.STRING
                        ? keyword()
                        : Property.of(p -> p.sparseVector(s -> s));
            default -> null;
        };
    }

    private Property decimalProperty(Schema schema) {
        return switch (config.decimalOutputMode()) {
            case STRING -> keyword();
            case DOUBLE -> Property.of(p -> p.double_(d -> d));
            case NUMERIC -> {
                final String scaleParameter = schema.parameters() != null ? schema.parameters().get(Decimal.SCALE_FIELD) : null;
                final int scale = scaleParameter != null ? Integer.parseInt(scaleParameter) : 0;
                yield scale > 0
                        ? Property.of(p -> p.scaledFloat(s -> s.scalingFactor(Math.pow(10, scale))))
                        : Property.of(p -> p.long_(l -> l));
            }
        };
    }

    private Property dateProperty(String isoFormat) {
        return switch (config.temporalOutputMode()) {
            case ISO8601 -> Property.of(p -> p.date(d -> d.format(isoFormat)));
            case EPOCH_MILLIS -> Property.of(p -> p.date(d -> d.format("epoch_millis")));
            case EPOCH_NANOS -> Property.of(p -> p.long_(l -> l));
        };
    }

    private Property jsonProperty() {
        if (config.jsonOutputMode() == JsonOutputMode.STRING) {
            return stringProperty();
        }
        return config.jsonMappingMode() == JsonMappingMode.FLATTENED
                ? Property.of(p -> p.flattened(f -> f))
                : dynamicObject();
    }

    private Property geometryProperty(boolean point) {
        if (config.geometryOutputMode() == GeometryOutputMode.WKB) {
            return dynamicObject();
        }
        return point ? Property.of(p -> p.geoPoint(g -> g)) : Property.of(p -> p.geoShape(g -> g));
    }

    private Property stringProperty() {
        return switch (config.stringMappingMode()) {
            case KEYWORD -> keyword();
            case TEXT -> Property.of(p -> p.text(t -> t));
            case TEXT_WITH_KEYWORD -> Property.of(p -> p.text(t -> t
                    .fields("keyword", Property.of(k -> k.keyword(kw -> kw.ignoreAbove(256))))));
        };
    }

    private Property structProperty(Schema schema) {
        final Map<String, Property> nested = new LinkedHashMap<>();
        for (Field field : schema.fields()) {
            nested.put(field.name(), property(field.schema()));
        }
        return config.structMappingMode() == StructMappingMode.NESTED
                ? Property.of(p -> p.nested(n -> n.properties(nested)))
                : Property.of(p -> p.object(o -> o.properties(nested)));
    }

    private Property dynamicObject() {
        return Property.of(p -> p.object(o -> o.dynamic(DynamicMapping.True)));
    }

    private Property keyword() {
        return Property.of(p -> p.keyword(k -> k));
    }
}
