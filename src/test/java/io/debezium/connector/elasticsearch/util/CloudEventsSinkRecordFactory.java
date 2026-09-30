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

/**
 * Wraps every Debezium envelope in a structured-mode CloudEvents envelope, as the CloudEvents
 * converter emits it: the change event travels in the {@code data} attribute and the value schema
 * name ends in {@code CloudEvents.Envelope}, which the shared record binding unwraps.
 *
 * @author Chris Cranford
 */
public class CloudEventsSinkRecordFactory extends DebeziumSinkRecordFactory {

    @Override
    public SinkRecord createRecord(String topic, int id, String name, String email, long offset) {
        return wrap(super.createRecord(topic, id, name, email, offset));
    }

    @Override
    public SinkRecord updateRecord(String topic, int id, String name, String email, long offset) {
        return wrap(super.updateRecord(topic, id, name, email, offset));
    }

    @Override
    public SinkRecord deleteRecord(String topic, int id, long offset) {
        return wrap(super.deleteRecord(topic, id, offset));
    }

    @Override
    public SinkRecord createRecordWithExtraField(String topic, int id, String name, String extraField, String extraValue, long offset) {
        return wrap(super.createRecordWithExtraField(topic, id, name, extraField, extraValue, offset));
    }

    @Override
    public SinkRecord createRecordWithCompositeKey(String topic, String tenant, int id, String name, long offset) {
        return wrap(super.createRecordWithCompositeKey(topic, tenant, id, name, offset));
    }

    @Override
    public SinkRecord truncateRecord(String topic, long offset) {
        return wrap(super.truncateRecord(topic, offset));
    }

    private SinkRecord wrap(SinkRecord envelope) {
        if (envelope.value() == null) {
            return envelope;
        }
        final Schema schema = SchemaBuilder.struct().name(envelope.topic() + ".CloudEvents.Envelope")
                .field("id", Schema.STRING_SCHEMA)
                .field("source", Schema.STRING_SCHEMA)
                .field("specversion", Schema.STRING_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .field("time", Schema.STRING_SCHEMA)
                .field("datacontenttype", Schema.STRING_SCHEMA)
                .field("data", envelope.valueSchema())
                .build();
        final Struct value = new Struct(schema)
                .put("id", envelope.topic() + ":" + envelope.kafkaOffset())
                .put("source", "/debezium/test/inventory")
                .put("specversion", "1.0")
                .put("type", "io.debezium.connector.test.DataChangeEvent")
                .put("time", Instant.now().toString())
                .put("datacontenttype", "application/json")
                .put("data", envelope.value());
        return new SinkRecord(envelope.topic(), envelope.kafkaPartition(), envelope.keySchema(), envelope.key(), schema, value,
                envelope.kafkaOffset());
    }
}
