/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.data.Envelope;

/**
 * Produces the flattened records {@code ExtractNewRecordState} emits with {@code add.headers=op}
 * and {@code delete.tombstone.handling.mode=rewrite}: the row fields alone in the value, with the
 * {@code __op} and {@code __deleted} markers carried as record headers.
 *
 * @author Chris Cranford
 */
public class FlatHeaderSinkRecordFactory extends FlatSinkRecordFactory {

    @Override
    public SinkRecord createRecord(String topic, int id, String name, String email, long offset) {
        return headed(topic, KEY_SCHEMA, key(id), ROW_SCHEMA, row(id, name, email), Envelope.Operation.CREATE, false, offset);
    }

    @Override
    public SinkRecord updateRecord(String topic, int id, String name, String email, long offset) {
        return headed(topic, KEY_SCHEMA, key(id), ROW_SCHEMA, row(id, name, email), Envelope.Operation.UPDATE, false, offset);
    }

    @Override
    public SinkRecord deleteRecord(String topic, int id, long offset) {
        return headed(topic, KEY_SCHEMA, key(id), ROW_SCHEMA, row(id, null, null), Envelope.Operation.DELETE, true, offset);
    }

    @Override
    public SinkRecord createRecordWithExtraField(String topic, int id, String name, String extraField, String extraValue, long offset) {
        final Schema schema = rowSchemaWith(extraField);
        final Struct value = new Struct(schema).put("id", id).put("name", name).put(extraField, extraValue);
        return headed(topic, KEY_SCHEMA, key(id), schema, value, Envelope.Operation.CREATE, false, offset);
    }

    @Override
    public SinkRecord createRecordWithCompositeKey(String topic, String tenant, int id, String name, long offset) {
        return headed(topic, COMPOSITE_KEY_SCHEMA, compositeKey(tenant, id), ROW_SCHEMA, row(id, name, null), Envelope.Operation.CREATE,
                false, offset);
    }

    private SinkRecord headed(String topic, Schema keySchema, Struct key, Schema valueSchema, Struct value, Envelope.Operation operation,
                              boolean deleted, long offset) {
        final Headers headers = new ConnectHeaders()
                .addString("__op", operation.code())
                .addString("__deleted", Boolean.toString(deleted));
        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset, null, TimestampType.NO_TIMESTAMP_TYPE, headers);
    }
}
