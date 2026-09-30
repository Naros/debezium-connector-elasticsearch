/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.filter.FieldFilterFactory;

/**
 * Unit tests for {@link ElasticsearchSinkRecord}: the primitive-key wrapper that lets the shared
 * deduplicating buffer key on non-struct record keys (DDD-variants.md entry 2).
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchSinkRecordTest {

    private static final String TOPIC = "inventory.customers";

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key")
            .field("tenant", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .build();
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    @Test
    void shouldWrapPrimitiveKeyInSyntheticStructUnderRecordKey() {
        final Struct filtered = record(Schema.INT64_SCHEMA, 42L)
                .getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);

        assertThat(filtered.schema().name()).isEqualTo("io.debezium.connector.elasticsearch.PrimitiveKey");
        assertThat(filtered.schema().fields()).hasSize(1);
        assertThat(filtered.schema().field("key").schema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(filtered.getInt64("key")).isEqualTo(42L);
    }

    @Test
    void shouldWrapStringKeyInSyntheticStructUnderRecordKey() {
        final Struct filtered = record(Schema.STRING_SCHEMA, "k-1")
                .getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);

        assertThat(filtered.getString("key")).isEqualTo("k-1");
    }

    @Test
    void shouldProduceEqualWrappersForEqualPrimitiveKeys() {
        // The buffer relies on struct equality to reduce records for the same key.
        final Struct first = record(Schema.INT32_SCHEMA, 7).getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);
        final Struct second = record(Schema.INT32_SCHEMA, 7).getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);
        final Struct other = record(Schema.INT32_SCHEMA, 8).getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);

        assertThat(first).isEqualTo(second);
        assertThat(first).isNotEqualTo(other);
    }

    @Test
    void shouldDelegateStructKeyUnderRecordKey() {
        final Struct key = new Struct(KEY_SCHEMA).put("tenant", "acme").put("id", 7);
        final Struct filtered = record(KEY_SCHEMA, key)
                .getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of("id"), FieldFilterFactory.DEFAULT_FILTER);

        assertThat(filtered.schema().fields()).hasSize(1);
        assertThat(filtered.getInt32("id")).isEqualTo(7);
    }

    @Test
    void shouldDelegateStructKeyWithAllFieldsWhenNoneConfigured() {
        final Struct key = new Struct(KEY_SCHEMA).put("tenant", "acme").put("id", 7);
        final Struct filtered = record(KEY_SCHEMA, key)
                .getFilteredKey(PrimaryKeyMode.RECORD_KEY, Set.of(), FieldFilterFactory.DEFAULT_FILTER);

        assertThat(filtered.getString("tenant")).isEqualTo("acme");
        assertThat(filtered.getInt32("id")).isEqualTo(7);
    }

    @Test
    void shouldDelegateForOtherModesWithPrimitiveKey() {
        final ElasticsearchSinkRecord record = record(Schema.INT32_SCHEMA, 7);

        assertThat(record.getFilteredKey(PrimaryKeyMode.NONE, Set.of(), FieldFilterFactory.DEFAULT_FILTER)).isNull();
        assertThat(record.getFilteredKey(PrimaryKeyMode.KAFKA, Set.of(), FieldFilterFactory.DEFAULT_FILTER))
                .isEqualTo(record.kafkaCoordinates());
        assertThat(record.getFilteredKey(PrimaryKeyMode.RECORD_VALUE, Set.of("id"), FieldFilterFactory.DEFAULT_FILTER).getInt32("id"))
                .isEqualTo(1);
    }

    private static ElasticsearchSinkRecord record(Schema keySchema, Object key) {
        final Struct value = new Struct(ROW_SCHEMA).put("id", 1).put("name", "alice");
        return new ElasticsearchSinkRecord(new SinkRecord(TOPIC, 0, keySchema, key, ROW_SCHEMA, value, 0), null);
    }
}
