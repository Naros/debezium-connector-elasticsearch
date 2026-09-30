/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

/**
 * Produces the flattened records {@code ExtractNewRecordState} emits with {@code add.fields=op}
 * and {@code delete.tombstone.handling.mode=rewrite}: the row fields plus {@code __op} and
 * {@code __deleted} markers.
 *
 * @author Chris Cranford
 */
public class FlatSinkRecordFactory implements SinkRecordFactory {

    private static final String OP_FIELD = "__op";
    private static final String DELETED_FIELD = "__deleted";

    @Override
    public boolean isFlattened() {
        return true;
    }

    @Override
    public SinkRecord createRecord(String topic, int id, String name, String email, long offset) {
        return flat(topic, id, Envelope.Operation.CREATE, false, flatSchema(null), name, email, null, null, offset);
    }

    @Override
    public SinkRecord updateRecord(String topic, int id, String name, String email, long offset) {
        return flat(topic, id, Envelope.Operation.UPDATE, false, flatSchema(null), name, email, null, null, offset);
    }

    @Override
    public SinkRecord deleteRecord(String topic, int id, long offset) {
        return flat(topic, id, Envelope.Operation.DELETE, true, flatSchema(null), null, null, null, null, offset);
    }

    @Override
    public SinkRecord createRecordWithExtraField(String topic, int id, String name, String extraField, String extraValue, long offset) {
        return flat(topic, id, Envelope.Operation.CREATE, false, flatSchema(extraField), name, null, extraField, extraValue, offset);
    }

    @Override
    public SinkRecord createRecordWithCompositeKey(String topic, String tenant, int id, String name, long offset) {
        final Schema schema = flatSchema(null);
        final Struct value = new Struct(schema)
                .put("id", id)
                .put("name", name)
                .put(OP_FIELD, Envelope.Operation.CREATE.code())
                .put(DELETED_FIELD, "false");
        return new SinkRecord(topic, 0, COMPOSITE_KEY_SCHEMA, compositeKey(tenant, id), schema, value, offset);
    }

    @Override
    public SinkRecord truncateRecord(String topic, long offset) {
        // ExtractNewRecordState drops truncates, so the only shape a sink can receive is the envelope.
        return new DebeziumSinkRecordFactory().truncateRecord(topic, offset);
    }

    private Schema flatSchema(String extraField) {
        final SchemaBuilder builder = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA);
        if (extraField != null) {
            builder.field(extraField, Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder
                .field(OP_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private SinkRecord flat(String topic, int id, Envelope.Operation operation, boolean deleted, Schema schema, String name, String email,
                            String extraField, String extraValue, long offset) {
        final Struct value = new Struct(schema)
                .put("id", id)
                .put("name", name)
                .put("email", email)
                .put(OP_FIELD, operation.code())
                .put(DELETED_FIELD, Boolean.toString(deleted));
        if (extraField != null) {
            value.put(extraField, extraValue);
        }
        return new SinkRecord(topic, 0, KEY_SCHEMA, key(id), schema, value, offset);
    }
}
