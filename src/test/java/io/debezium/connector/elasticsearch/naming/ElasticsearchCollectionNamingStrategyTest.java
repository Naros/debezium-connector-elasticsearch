/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.DebeziumException;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * Unit tests for {@link ElasticsearchCollectionNamingStrategy}: the placeholder vocabulary of
 * DDD-61 section 4.1, the topic override, and the three invalid-name handling modes.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchCollectionNamingStrategyTest {

    private static final String TOPIC = "server.inventory.customers";
    private static final String CLOUD_EVENTS_PATTERN = ".*CloudEvents\\.Envelope$";
    // 2024-03-15T23:30:00Z: still the 15th in UTC, already the 16th in Tokyo.
    private static final long SOURCE_TS_MS = 1710545400000L;

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema ATTRIBUTES_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
    private static final Schema ADDRESS_SCHEMA = SchemaBuilder.struct().name("Address")
            .field("country", Schema.STRING_SCHEMA)
            .optional()
            .build();
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("tenant", Schema.OPTIONAL_STRING_SCHEMA)
            .field("address", ADDRESS_SCHEMA)
            .field("attributes", ATTRIBUTES_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("Source")
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();
    private static final Schema FLAT_SCHEMA = SchemaBuilder.struct().name("Flat")
            .field("id", Schema.INT32_SCHEMA)
            .field("tenant", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__source_db", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__source_table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    @Test
    void shouldResolveTopicPlaceholder() {
        assertThat(strategy(Map.of()).resolveCollectionName(envelopeRecord(), "${topic}")).isEqualTo(TOPIC);
        assertThat(strategy(Map.of()).resolveCollectionName(envelopeRecord(), "prefix-${topic}-suffix"))
                .isEqualTo("prefix-" + TOPIC + "-suffix");
    }

    @ParameterizedTest
    @CsvSource({
            "${source.db}, inventory",
            "${source.schema}, public",
            "${source.table}, customers",
            "${source.connector}, postgresql",
            "${source.db}.${source.table}, inventory.customers"
    })
    void shouldResolveSourcePlaceholdersFromEnvelope(String format, String expected) {
        assertThat(strategy(Map.of()).resolveCollectionName(envelopeRecord(), format)).isEqualTo(expected);
    }

    @Test
    void shouldResolveSourcePlaceholdersFromFlattenedSourceFields() {
        final DebeziumSinkRecord record = flatRecord("inventory", "customers", null);
        assertThat(strategy(Map.of()).resolveCollectionName(record, "${source.db}-${source.table}"))
                .isEqualTo("inventory-customers");
    }

    @Test
    void shouldRejectUnknownSourcePlaceholder() {
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(envelopeRecord(), "${source.host}"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("${source.host}");
    }

    @Test
    void shouldRejectSourcePlaceholderWhenRecordCarriesNoSource() {
        final DebeziumSinkRecord record = flatRecord(null, null, null);
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(record, "${source.db}"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("${source.db}")
                .hasMessageContaining(TOPIC);
    }

    @Test
    void shouldResolveFieldPlaceholderThroughStructAndMap() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of());
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "${field:tenant}")).isEqualTo("acme");
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "${field:address.country}")).isEqualTo("de");
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "${field:attributes.region}")).isEqualTo("eu");
        assertThat(strategy.resolveCollectionName(flatRecord(null, null, "acme"), "${field:tenant}")).isEqualTo("acme");
    }

    @ParameterizedTest
    @CsvSource({ "${field:missing}", "${field:address.missing}", "${field:attributes.missing}", "${field:tenant.deeper}" })
    void shouldRejectUnresolvableFieldPlaceholder(String format) {
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(envelopeRecord(), format))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining(format);
    }

    @Test
    void shouldResolveHeaderPlaceholder() {
        final Headers headers = new ConnectHeaders().addString("region", "eu-west");
        final DebeziumSinkRecord record = wrap(new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(), FLAT_SCHEMA, flatValue(null, null, null),
                0, null, TimestampType.NO_TIMESTAMP_TYPE, headers));
        assertThat(strategy(Map.of()).resolveCollectionName(record, "orders-${header:region}")).isEqualTo("orders-eu-west");
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(record, "${header:absent}"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("${header:absent}");
    }

    @Test
    void shouldResolveDatePlaceholderFromSourceTimestampInUtcByDefault() {
        assertThat(strategy(Map.of()).resolveCollectionName(envelopeRecord(), "customers-${date:yyyy.MM.dd}"))
                .isEqualTo("customers-2024.03.15");
    }

    @Test
    void shouldResolveDatePlaceholderInConfiguredTimezone() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of(
                ElasticsearchSinkConnectorConfig.RESOURCE_NAME_TIMEZONE, "Asia/Tokyo"));
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "customers-${date:yyyy.MM.dd}"))
                .isEqualTo("customers-2024.03.16");
    }

    @Test
    void shouldResolveDatePlaceholderFromKafkaTimestampWhenRecordHasNoSource() {
        final DebeziumSinkRecord record = wrap(new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(), FLAT_SCHEMA, flatValue(null, null, null),
                0, SOURCE_TS_MS, TimestampType.CREATE_TIME));
        assertThat(strategy(Map.of()).resolveCollectionName(record, "${date:yyyy-MM}")).isEqualTo("2024-03");
    }

    @Test
    void shouldRejectDatePlaceholderWithoutAnyTimestamp() {
        final DebeziumSinkRecord record = flatRecord(null, null, null);
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(record, "${date:yyyy}"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("${date:...}");
    }

    @Test
    void shouldRejectUnknownPlaceholder() {
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(envelopeRecord(), "${nonsense}"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("${nonsense}");
    }

    @Test
    void shouldPreferTopicToResourceMappingOverFormat() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of(
                ElasticsearchSinkConnectorConfig.TOPIC_TO_RESOURCE_MAPPING, "other:x, " + TOPIC + " : customers-index"));
        // DDD-61 4.2: the mapping is an override consulted before the format, unconditionally.
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "${field:missing}")).isEqualTo("customers-index");
    }

    @Test
    void shouldResolveExpressionWithoutApplyingNameRules() {
        // An ingest pipeline name may legitimately contain characters an index name cannot.
        assertThat(strategy(Map.of()).resolveExpression(envelopeRecord(), "Pipeline:${source.table}"))
                .isEqualTo("Pipeline:customers");
    }

    @Test
    void shouldFailOnInvalidNameByDefault() {
        assertThatThrownBy(() -> strategy(Map.of()).resolveCollectionName(envelopeRecord(), "Customers/${source.table}"))
                .isInstanceOf(DebeziumException.class)
                .isNotInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("Customers/customers")
                .hasMessageContaining("lowercase")
                .hasMessageContaining(ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING);
    }

    @Test
    void shouldRouteInvalidNameToErrorHandlerWhenConfigured() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of(
                ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING, "error_handler"));
        assertThatThrownBy(() -> strategy.resolveCollectionName(envelopeRecord(), "Customers"))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("Customers")
                .hasMessageContaining(TOPIC);
    }

    @Test
    void shouldSanitizeInvalidNameWhenConfigured() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of(
                ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING, "sanitize",
                ElasticsearchSinkConnectorConfig.RESOURCE_NAME_REPLACEMENT, "-"));
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "Inventory/${source.table}")).isEqualTo("inventory-customers");
        assertThat(strategy.resolveCollectionName(envelopeRecord(), "${source.table}")).isEqualTo("customers");
    }

    @Test
    void shouldRejectNameThatRemainsInvalidAfterSanitization() {
        final ElasticsearchCollectionNamingStrategy strategy = strategy(Map.of(
                ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING, "sanitize"));
        assertThatThrownBy(() -> strategy.resolveCollectionName(envelopeRecord(), "."))
                .isInstanceOf(ResourceResolutionException.class)
                .hasMessageContaining("remains invalid");
    }

    private static ElasticsearchCollectionNamingStrategy strategy(Map<String, String> properties) {
        final ElasticsearchCollectionNamingStrategy strategy = new ElasticsearchCollectionNamingStrategy();
        strategy.configure(properties);
        return strategy;
    }

    private static DebeziumSinkRecord envelopeRecord() {
        final Envelope envelope = Envelope.defineSchema()
                .withName(TOPIC + ".Envelope")
                .withRecord(ROW_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();
        final Struct after = new Struct(ROW_SCHEMA)
                .put("id", 1)
                .put("tenant", "acme")
                .put("address", new Struct(ADDRESS_SCHEMA).put("country", "de"))
                .put("attributes", Map.of("region", "eu"));
        final Struct source = new Struct(SOURCE_SCHEMA)
                .put("version", "test")
                .put("connector", "postgresql")
                .put("name", "server")
                .put("ts_ms", SOURCE_TS_MS)
                .put("db", "inventory")
                .put("schema", "public")
                .put("table", "customers");
        final Struct value = envelope.create(after, source, Instant.ofEpochMilli(SOURCE_TS_MS));
        return wrap(new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(), envelope.schema(), value, 0));
    }

    private static DebeziumSinkRecord flatRecord(String sourceDb, String sourceTable, String tenant) {
        return wrap(new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(), FLAT_SCHEMA, flatValue(sourceDb, sourceTable, tenant), 0));
    }

    private static Struct flatValue(String sourceDb, String sourceTable, String tenant) {
        return new Struct(FLAT_SCHEMA)
                .put("id", 1)
                .put("tenant", tenant)
                .put("__source_db", sourceDb)
                .put("__source_table", sourceTable);
    }

    private static Struct key() {
        return new Struct(KEY_SCHEMA).put("id", 1);
    }

    private static DebeziumSinkRecord wrap(SinkRecord record) {
        return new ElasticsearchSinkRecord(record, CLOUD_EVENTS_PATTERN);
    }
}
