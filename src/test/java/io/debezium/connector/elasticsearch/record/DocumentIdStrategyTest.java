/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;

/**
 * Unit tests for {@link DocumentIdStrategy}: the {@code _id} derivation rules of DDD-61 3 under
 * every {@code primary.key.mode}.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class DocumentIdStrategyTest {

    private static final String TOPIC = "inventory.customers";

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("tenant", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema COMPOSITE_KEY_SCHEMA = SchemaBuilder.struct().name("Key")
            .field("tenant", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .build();

    @Test
    void shouldReturnEmptyUnderNoneEvenWithKey() {
        assertThat(strategy(Map.of("primary.key.mode", "none")).documentId(structKeyed("acme", 7))).isEmpty();
    }

    @Test
    void shouldDeriveTopicPartitionOffsetUnderKafka() {
        final SinkRecord record = new SinkRecord(TOPIC, 3, null, null, ROW_SCHEMA, row("acme", 7), 42);

        assertThat(strategy(Map.of("primary.key.mode", "kafka")).documentId(wrap(record))).contains(TOPIC + ":3:42");
    }

    @Test
    void shouldJoinStructKeyInSchemaOrderWhenFieldsUnset() {
        assertThat(strategy(Map.of()).documentId(structKeyed("acme", 7))).contains("acme:7");
    }

    @ParameterizedTest
    @ValueSource(strings = { "id,tenant", " id , tenant " })
    void shouldJoinStructKeyInConfiguredOrder(String fields) {
        assertThat(strategy(Map.of("primary.key.fields", fields)).documentId(structKeyed("acme", 7))).contains("7:acme");
    }

    @Test
    void shouldJoinStructKeyInConfiguredOrderMatchingSchema() {
        assertThat(strategy(Map.of("primary.key.fields", "tenant,id")).documentId(structKeyed("acme", 7))).contains("acme:7");
    }

    @Test
    void shouldUseConfiguredSeparator() {
        assertThat(strategy(Map.of("document.id.separator", "-")).documentId(structKeyed("acme", 7))).contains("acme-7");
    }

    @Test
    void shouldSelectSubsetOfStructKeyFields() {
        assertThat(strategy(Map.of("primary.key.fields", "id")).documentId(structKeyed("acme", 7))).contains("7");
    }

    @Test
    void shouldDeriveFromRecordValueFields() {
        final DocumentIdStrategy strategy = strategy(Map.of("primary.key.mode", "record_value", "primary.key.fields", "id,tenant"));

        assertThat(strategy.documentId(structKeyed("acme", 7))).contains("7:acme");
    }

    @Test
    void shouldDeriveFromRecordHeaders() {
        final Headers headers = new ConnectHeaders().addString("tenant", "acme").addInt("id", 7);
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, ROW_SCHEMA, row("acme", 7), 0, null,
                TimestampType.NO_TIMESTAMP_TYPE, headers);
        final DocumentIdStrategy strategy = strategy(Map.of("primary.key.mode", "record_header", "primary.key.fields", "tenant,id"));

        assertThat(strategy.documentId(wrap(record))).contains("acme:7");
    }

    static Stream<Arguments> primitiveKeys() {
        final byte[] bytes = { 1, 2, 3 };
        return Stream.of(
                Arguments.of(Schema.INT8_SCHEMA, (byte) 1, "1"),
                Arguments.of(Schema.INT16_SCHEMA, (short) 2, "2"),
                Arguments.of(Schema.INT32_SCHEMA, 3, "3"),
                Arguments.of(Schema.INT64_SCHEMA, 4L, "4"),
                Arguments.of(Schema.FLOAT32_SCHEMA, 1.5f, "1.5"),
                Arguments.of(Schema.FLOAT64_SCHEMA, 2.25d, "2.25"),
                Arguments.of(Schema.BOOLEAN_SCHEMA, true, "true"),
                Arguments.of(Schema.STRING_SCHEMA, "k-1", "k-1"),
                Arguments.of(Schema.BYTES_SCHEMA, bytes, Base64.getEncoder().encodeToString(bytes)),
                Arguments.of(Schema.BYTES_SCHEMA, ByteBuffer.wrap(bytes), Base64.getEncoder().encodeToString(bytes)),
                Arguments.of(Decimal.schema(2), new BigDecimal("12.50"), "12.50"),
                Arguments.of(Decimal.schema(0), new BigDecimal("1E+3"), "1000"));
    }

    @ParameterizedTest
    @MethodSource("primitiveKeys")
    void shouldStringifyPrimitiveKey(Schema keySchema, Object key, String expected) {
        assertThat(strategy(Map.of()).documentId(keyed(keySchema, key))).contains(expected);
    }

    @Test
    void shouldStringifyDecimalFieldOfStructKeyWithoutExponent() {
        final Schema keySchema = SchemaBuilder.struct().field("amount", Decimal.schema(0)).build();
        final Struct key = new Struct(keySchema).put("amount", new BigDecimal("1E+2"));

        assertThat(strategy(Map.of()).documentId(keyed(keySchema, key))).contains("100");
    }

    @Test
    void shouldAcceptIdOfExactlyMaximumBytes() {
        assertThat(strategy(Map.of()).documentId(keyed(Schema.STRING_SCHEMA, "x".repeat(512)))).hasValueSatisfying(
                id -> assertThat(id).hasSize(512));
    }

    @Test
    void shouldRejectIdOverMaximumBytes() {
        assertThatThrownBy(() -> strategy(Map.of()).documentId(keyed(Schema.STRING_SCHEMA, "x".repeat(513))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("513 bytes")
                .hasMessageContaining("512-byte limit");
    }

    @Test
    void shouldMeasureLimitInUtf8BytesNotCharacters() {
        // 171 three-byte characters are 513 bytes.
        assertThatThrownBy(() -> strategy(Map.of()).documentId(keyed(Schema.STRING_SCHEMA, "€".repeat(171))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("513 bytes");
    }

    @Test
    void shouldRejectNullKeyUnderRecordKey() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, ROW_SCHEMA, row("acme", 7), 0);

        assertThatThrownBy(() -> strategy(Map.of()).documentId(wrap(record)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("resolved to null")
                .hasMessageContaining("'primary.key.mode=record_key'");
    }

    @Test
    void shouldRejectMissingConfiguredField() {
        assertThatThrownBy(() -> strategy(Map.of("primary.key.fields", "id,region")).documentId(structKeyed("acme", 7)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("'region' does not exist in the record key");
    }

    @Test
    void shouldRejectNullFieldValue() {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.OPTIONAL_INT32_SCHEMA).build();
        final Struct key = new Struct(keySchema);

        assertThatThrownBy(() -> strategy(Map.of()).documentId(keyed(keySchema, key)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("'id' is null in the record key");
    }

    @Test
    void shouldRejectNonStructNonPrimitiveKey() {
        final Schema keySchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        assertThatThrownBy(() -> strategy(Map.of()).documentId(keyed(keySchema, List.of(1, 2))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("Unsupported record key schema type 'ARRAY'");
    }

    @Test
    void shouldRejectRecordValueModeWhenPayloadIsAbsent() {
        final SinkRecord tombstone = new SinkRecord(TOPIC, 0, Schema.INT32_SCHEMA, 1, null, null, 0);
        final DocumentIdStrategy strategy = strategy(Map.of("primary.key.mode", "record_value", "primary.key.fields", "id"));

        assertThatThrownBy(() -> strategy.documentId(wrap(tombstone)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("record value is absent");
    }

    @Test
    void shouldRejectRecordHeaderModeWhenHeaderIsMissing() {
        final DocumentIdStrategy strategy = strategy(Map.of("primary.key.mode", "record_header", "primary.key.fields", "id"));

        assertThatThrownBy(() -> strategy.documentId(structKeyed("acme", 7)))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("'id' does not exist in the record headers");
    }

    private static DocumentIdStrategy strategy(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "http://localhost:9200");
        properties.putAll(overrides);
        return new DocumentIdStrategy(new ElasticsearchSinkConnectorConfig(properties));
    }

    private static ElasticsearchSinkRecord wrap(SinkRecord record) {
        return new ElasticsearchSinkRecord(record, null);
    }

    private static ElasticsearchSinkRecord keyed(Schema keySchema, Object key) {
        return wrap(new SinkRecord(TOPIC, 0, keySchema, key, ROW_SCHEMA, row("acme", 7), 0));
    }

    private static ElasticsearchSinkRecord structKeyed(String tenant, int id) {
        final Struct key = new Struct(COMPOSITE_KEY_SCHEMA).put("tenant", tenant).put("id", id);
        return keyed(COMPOSITE_KEY_SCHEMA, key);
    }

    private static Struct row(String tenant, int id) {
        return new Struct(ROW_SCHEMA).put("tenant", tenant).put("id", id).put("name", "alice");
    }
}
