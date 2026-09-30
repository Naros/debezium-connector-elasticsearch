/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

/**
 * Produces Debezium change event envelopes with {@code before}, {@code after}, {@code source},
 * {@code op}, and {@code ts_ms}.
 *
 * @author Chris Cranford
 */
public class DebeziumSinkRecordFactory implements SinkRecordFactory {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("Source")
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();

    @Override
    public boolean isFlattened() {
        return false;
    }

    @Override
    public SinkRecord createRecord(String topic, int id, String name, String email, long offset) {
        return envelope(topic, id, Envelope.Operation.CREATE, ROW_SCHEMA, null, row(id, name, email), offset);
    }

    @Override
    public SinkRecord updateRecord(String topic, int id, String name, String email, long offset) {
        return envelope(topic, id, Envelope.Operation.UPDATE, ROW_SCHEMA, row(id, null, null), row(id, name, email), offset);
    }

    @Override
    public SinkRecord deleteRecord(String topic, int id, long offset) {
        return envelope(topic, id, Envelope.Operation.DELETE, ROW_SCHEMA, row(id, null, null), null, offset);
    }

    @Override
    public SinkRecord createRecordWithExtraField(String topic, int id, String name, String extraField, String extraValue, long offset) {
        final Schema rowSchema = rowSchemaWith(extraField);
        final Struct after = new Struct(rowSchema).put("id", id).put("name", name).put(extraField, extraValue);
        return envelope(topic, id, Envelope.Operation.CREATE, rowSchema, null, after, offset);
    }

    @Override
    public SinkRecord createRecordWithCompositeKey(String topic, String tenant, int id, String name, long offset) {
        return envelope(topic, COMPOSITE_KEY_SCHEMA, compositeKey(tenant, id), Envelope.Operation.CREATE, ROW_SCHEMA, null,
                row(id, name, null), offset);
    }

    @Override
    public SinkRecord truncateRecord(String topic, long offset) {
        return envelope(topic, null, null, Envelope.Operation.TRUNCATE, ROW_SCHEMA, null, null, offset);
    }

    private SinkRecord envelope(String topic, int id, Envelope.Operation operation, Schema rowSchema, Struct before, Struct after,
                                long offset) {
        return envelope(topic, KEY_SCHEMA, key(id), operation, rowSchema, before, after, offset);
    }

    private SinkRecord envelope(String topic, Schema keySchema, Struct key, Envelope.Operation operation, Schema rowSchema, Struct before,
                                Struct after, long offset) {
        final Schema optionalRow = optional(rowSchema);
        before = reschema(before, optionalRow);
        after = reschema(after, optionalRow);
        final Envelope envelope = Envelope.defineSchema()
                .withName(topic + ".Envelope")
                .withRecord(optionalRow)
                .withSource(SOURCE_SCHEMA)
                .build();
        final Struct source = new Struct(SOURCE_SCHEMA)
                .put("version", "test")
                .put("connector", "test")
                .put("name", "test")
                .put("ts_ms", Instant.now().toEpochMilli())
                .put("db", "inventory")
                .put("table", "customers");
        final Instant now = Instant.now();
        final Struct value = switch (operation) {
            case CREATE -> envelope.create(after, source, now);
            case UPDATE -> envelope.update(before, after, source, now);
            case DELETE -> envelope.delete(before, source, now);
            case TRUNCATE -> envelope.truncate(source, now);
            default -> throw new IllegalArgumentException("Unsupported operation " + operation);
        };
        return new SinkRecord(topic, 0, keySchema, key, envelope.schema(), value, offset);
    }

    /**
     * Source connectors declare {@code before} and {@code after} optional; a required row schema
     * would fail struct validation on a create (no {@code before}) or a delete (no {@code after}).
     */
    private static Struct reschema(Struct struct, Schema schema) {
        if (struct == null) {
            return null;
        }
        final Struct copy = new Struct(schema);
        struct.schema().fields().forEach(field -> copy.put(field.name(), struct.get(field)));
        return copy;
    }

    private static Schema optional(Schema rowSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct().name(rowSchema.name()).optional();
        rowSchema.fields().forEach(field -> builder.field(field.name(), field.schema()));
        return builder.build();
    }
}
