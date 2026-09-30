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

/**
 * Builds the {@link SinkRecord}s a test feeds the task. One implementation produces Debezium
 * envelopes and another the flattened shape {@code ExtractNewRecordState} produces, so the same
 * test can assert both input contracts converge on the same index state.
 *
 * @author Chris Cranford
 */
public interface SinkRecordFactory {

    Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();

    /**
     * A two-field key whose schema order ({@code tenant}, {@code id}) differs from the order tests
     * configure in {@code primary.key.fields}, so composition order is observable.
     */
    Schema COMPOSITE_KEY_SCHEMA = SchemaBuilder.struct().name("CompositeKey")
            .field("tenant", Schema.STRING_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .build();

    Schema ROW_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    /**
     * Whether records are the flattened shape rather than Debezium envelopes.
     */
    boolean isFlattened();

    SinkRecord createRecord(String topic, int id, String name, String email, long offset);

    SinkRecord updateRecord(String topic, int id, String name, String email, long offset);

    SinkRecord deleteRecord(String topic, int id, long offset);

    /**
     * A create whose row carries one field beyond {@link #ROW_SCHEMA}, for schema-shape tests.
     */
    SinkRecord createRecordWithExtraField(String topic, int id, String name, String extraField, String extraValue, long offset);

    /**
     * A create keyed by {@link #COMPOSITE_KEY_SCHEMA}.
     */
    SinkRecord createRecordWithCompositeKey(String topic, String tenant, int id, String name, long offset);

    /**
     * A Debezium truncate event for the topic: an envelope with {@code op=t}, a source, and no
     * key. There is no flattened form, so every factory produces the envelope.
     */
    SinkRecord truncateRecord(String topic, long offset);

    default Struct compositeKey(String tenant, int id) {
        return new Struct(COMPOSITE_KEY_SCHEMA).put("tenant", tenant).put("id", id);
    }

    default Struct key(int id) {
        return new Struct(KEY_SCHEMA).put("id", id);
    }

    default Struct row(int id, String name, String email) {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", name).put("email", email);
    }

    default Schema rowSchemaWith(String extraField) {
        return SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("email", Schema.OPTIONAL_STRING_SCHEMA)
                .field(extraField, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }
}
