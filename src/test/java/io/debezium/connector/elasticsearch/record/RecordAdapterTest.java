/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.data.Envelope;

/**
 * Unit tests for {@link RecordAdapter}: the three input contracts of DDD-61 2 and the operation
 * each record shape resolves to.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class RecordAdapterTest {

    private static final String TOPIC = "inventory.customers";
    private static final String CE_PATTERN = ".*CloudEvents\\.Envelope$";

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("Source")
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();
    private static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName(TOPIC + ".Envelope")
            .withRecord(ROW_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .build();
    private static final Schema FLAT_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__op", Schema.OPTIONAL_STRING_SCHEMA)
            .field("__deleted", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    static Stream<Arguments> envelopeWrites() {
        return Stream.of(
                Arguments.of(Envelope.Operation.CREATE),
                Arguments.of(Envelope.Operation.READ),
                Arguments.of(Envelope.Operation.UPDATE));
    }

    @ParameterizedTest
    @MethodSource("envelopeWrites")
    void shouldAdaptEnvelopeCreateReadAndUpdateToWriteOfAfter(Envelope.Operation operation) {
        final SinkOperation result = adapter().adapt(record(envelope(operation)));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        final SinkOperation.Write write = (SinkOperation.Write) result;
        assertThat(write.operation()).isEqualTo(operation);
        assertThat(write.payload().getInt32("id")).isEqualTo(1);
        assertThat(write.payload().getString("name")).isEqualTo("after");
        assertThat(write.rawValue()).isNull();
    }

    @Test
    void shouldAdaptEnvelopeDeleteToDelete() {
        assertThat(adapter().adapt(record(envelope(Envelope.Operation.DELETE)))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldAdaptEnvelopeTruncateToTruncate() {
        assertThat(adapter().adapt(record(envelope(Envelope.Operation.TRUNCATE)))).isInstanceOf(SinkOperation.Truncate.class);
    }

    @Test
    void shouldSkipEnvelopeMessageEvent() {
        final SinkOperation result = adapter().adapt(record(envelope(Envelope.Operation.MESSAGE)));

        assertThat(result).isInstanceOf(SinkOperation.Skip.class);
        assertThat(((SinkOperation.Skip) result).reason()).contains("'m'");
    }

    @Test
    void shouldRejectUnknownEnvelopeOperation() {
        final Struct value = new Struct(ENVELOPE.schema())
                .put(Envelope.FieldName.OPERATION, "x")
                .put(Envelope.FieldName.SOURCE, source());

        assertThatThrownBy(() -> adapter().adapt(record(sinkRecord(value.schema(), value))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("Unknown envelope operation 'x'");
    }

    @Test
    void shouldAdaptFlattenedRecordWithOpFieldToWrite() {
        final SinkOperation result = adapter().adapt(record(sinkRecord(FLAT_SCHEMA, flat("u", "false"))));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        final SinkOperation.Write write = (SinkOperation.Write) result;
        assertThat(write.operation()).isEqualTo(Envelope.Operation.UPDATE);
        assertThat(write.payload().getString("name")).isEqualTo("flat");
    }

    @Test
    void shouldAdaptFlattenedRecordWithDeleteOpFieldToDelete() {
        assertThat(adapter().adapt(record(sinkRecord(FLAT_SCHEMA, flat("d", null))))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldAdaptFlattenedRecordWithDeletedFieldToDelete() {
        assertThat(adapter().adapt(record(sinkRecord(FLAT_SCHEMA, flat(null, "true"))))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldAdaptFlattenedNullValueWithSchemaToDelete() {
        // DDD-61 2.2 (ExtractNewRecordState with delete.handling.mode=none): a null value that
        // still carries the flattened schema is the delete signal on this path.
        assertThat(adapter().adapt(record(sinkRecord(ROW_SCHEMA, null)))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldReadOpMarkerFromHeadersWhenAbsentFromValue() {
        final Headers headers = new ConnectHeaders().addString("__op", "d");

        assertThat(adapter().adapt(record(sinkRecord(ROW_SCHEMA, row(1, "x"), headers)))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldReadDeletedMarkerFromHeadersWhenAbsentFromValue() {
        final Headers headers = new ConnectHeaders().addString("__deleted", "true");

        assertThat(adapter().adapt(record(sinkRecord(ROW_SCHEMA, row(1, "x"), headers)))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldAdaptHeaderMarkedWriteWithOperationFromHeader() {
        final Headers headers = new ConnectHeaders().addString("__op", "c").addString("__deleted", "false");
        final SinkOperation result = adapter().adapt(record(sinkRecord(ROW_SCHEMA, row(1, "x"), headers)));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        assertThat(((SinkOperation.Write) result).operation()).isEqualTo(Envelope.Operation.CREATE);
    }

    @Test
    void shouldPreferValueMarkerOverHeaderMarker() {
        final Headers headers = new ConnectHeaders().addString("__deleted", "true").addString("__op", "d");
        final SinkOperation result = adapter().adapt(record(sinkRecord(FLAT_SCHEMA, flat("u", "false"), headers)));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        assertThat(((SinkOperation.Write) result).operation()).isEqualTo(Envelope.Operation.UPDATE);
    }

    @Test
    void shouldAdaptPlainStructToWriteWithoutOperation() {
        final SinkOperation result = adapter().adapt(record(sinkRecord(ROW_SCHEMA, row(1, "plain"))));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        final SinkOperation.Write write = (SinkOperation.Write) result;
        assertThat(write.operation()).isNull();
        assertThat(write.payload().getString("name")).isEqualTo("plain");
    }

    @Test
    void shouldAdaptSchemalessMapToRawWrite() {
        final Map<String, Object> value = Map.of("id", 1, "name", "raw");
        final SinkOperation result = adapter().adapt(record(sinkRecord(null, value)));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        final SinkOperation.Write write = (SinkOperation.Write) result;
        assertThat(write.payload()).isNull();
        assertThat(write.rawValue()).isEqualTo(value);
    }

    @Test
    void shouldAdaptSchemalessScalarToRawWrite() {
        final SinkOperation result = adapter().adapt(record(sinkRecord(null, "text")));

        assertThat(((SinkOperation.Write) result).rawValue()).isEqualTo("text");
    }

    @Test
    void shouldRejectPlainRecordWhenEnvelopeIsRequired() {
        assertThatThrownBy(() -> adapter(Map.of("event.format", "debezium")).adapt(record(sinkRecord(ROW_SCHEMA, row(1, "plain")))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("'event.format' is 'debezium'");
    }

    @Test
    void shouldStillAcceptEnvelopeWhenEnvelopeIsRequired() {
        assertThat(adapter(Map.of("event.format", "debezium")).adapt(record(envelope(Envelope.Operation.DELETE))))
                .isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldTreatEnvelopeAsOpaqueBodyUnderPlainFormat() {
        // DDD-61 2: 'plain' treats every value as an opaque document body, even one that looks
        // like an envelope, so the payload is the whole envelope rather than its 'after'.
        final SinkOperation result = adapter(Map.of("event.format", "plain")).adapt(record(envelope(Envelope.Operation.DELETE)));

        assertThat(result).isInstanceOf(SinkOperation.Write.class);
        final SinkOperation.Write write = (SinkOperation.Write) result;
        assertThat(write.operation()).isNull();
        assertThat(write.payload().schema().name()).isEqualTo(TOPIC + ".Envelope");
    }

    @ParameterizedTest
    @ValueSource(strings = { "auto", "delete" })
    void shouldDeleteOnTombstone(String mode) {
        assertThat(adapter(Map.of("tombstone.mode", mode)).adapt(record(tombstone(true)))).isInstanceOf(SinkOperation.Delete.class);
    }

    @Test
    void shouldSkipTombstoneWhenIgnored() {
        final SinkOperation result = adapter(Map.of("tombstone.mode", "ignore")).adapt(record(tombstone(true)));

        assertThat(result).isInstanceOf(SinkOperation.Skip.class);
        assertThat(((SinkOperation.Skip) result).reason()).contains("ignore");
    }

    @Test
    void shouldFailOnTombstoneWhenConfiguredToFail() {
        assertThatThrownBy(() -> adapter(Map.of("tombstone.mode", "fail")).adapt(record(tombstone(true))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("'tombstone.mode' is 'fail'");
    }

    @ParameterizedTest
    @ValueSource(strings = { "auto", "delete", "fail" })
    void shouldFailOnTombstoneWithNullKeyUnlessIgnored(String mode) {
        assertThatThrownBy(() -> adapter(Map.of("tombstone.mode", mode)).adapt(record(tombstone(false))))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("null key");
    }

    @Test
    void shouldSkipTombstoneWithNullKeyWhenIgnored() {
        assertThat(adapter(Map.of("tombstone.mode", "ignore")).adapt(record(tombstone(false)))).isInstanceOf(SinkOperation.Skip.class);
    }

    @Test
    void shouldDeleteOnTombstoneUnderPlainFormat() {
        // DDD-61 2.3: on the plain path a null value with a non-null key is a delete by default.
        assertThat(adapter(Map.of("event.format", "plain")).adapt(record(tombstone(true)))).isInstanceOf(SinkOperation.Delete.class);
    }

    private static RecordAdapter adapter() {
        return adapter(Map.of());
    }

    private static RecordAdapter adapter(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "http://localhost:9200");
        properties.putAll(overrides);
        return new RecordAdapter(new ElasticsearchSinkConnectorConfig(properties));
    }

    private static ElasticsearchSinkRecord record(SinkRecord record) {
        return new ElasticsearchSinkRecord(record, CE_PATTERN);
    }

    private static SinkRecord envelope(Envelope.Operation operation) {
        final Struct before = row(1, "before");
        final Struct after = row(1, "after");
        final Instant now = Instant.now();
        final Struct value = switch (operation) {
            case CREATE -> ENVELOPE.create(after, source(), now);
            case READ -> ENVELOPE.read(after, source(), now);
            case UPDATE -> ENVELOPE.update(before, after, source(), now);
            case DELETE -> ENVELOPE.delete(before, source(), now);
            case TRUNCATE -> ENVELOPE.truncate(source(), now);
            case MESSAGE -> new Struct(ENVELOPE.schema())
                    .put(Envelope.FieldName.OPERATION, Envelope.Operation.MESSAGE.code())
                    .put(Envelope.FieldName.SOURCE, source());
        };
        return sinkRecord(ENVELOPE.schema(), value);
    }

    private static SinkRecord tombstone(boolean withKey) {
        return new SinkRecord(TOPIC, 0, withKey ? KEY_SCHEMA : null, withKey ? key(1) : null, null, null, 0);
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Object value) {
        return new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(1), valueSchema, value, 0);
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Object value, Headers headers) {
        return new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(1), valueSchema, value, 0, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    private static Struct key(int id) {
        return new Struct(KEY_SCHEMA).put("id", id);
    }

    private static Struct row(int id, String name) {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", name);
    }

    private static Struct flat(String op, String deleted) {
        return new Struct(FLAT_SCHEMA).put("id", 1).put("name", "flat").put("__op", op).put("__deleted", deleted);
    }

    private static Struct source() {
        return new Struct(SOURCE_SCHEMA)
                .put("version", "test")
                .put("connector", "test")
                .put("name", "test")
                .put("ts_ms", Instant.now().toEpochMilli())
                .put("db", "inventory")
                .put("table", "customers");
    }
}
