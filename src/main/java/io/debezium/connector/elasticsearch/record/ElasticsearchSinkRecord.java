/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;

/**
 * Sink record binding that additionally supports primitive record keys under
 * {@code primary.key.mode=record_key} by wrapping them in a synthetic single-field struct, so
 * the deduplicating buffer can key on them; the shared binding supports only struct keys there.
 *
 * @author Chris Cranford
 */
public class ElasticsearchSinkRecord extends KafkaDebeziumSinkRecord {

    private static final String PRIMITIVE_KEY_FIELD = "key";

    public ElasticsearchSinkRecord(SinkRecord record, String cloudEventsSchemaNamePattern) {
        super(record, cloudEventsSchemaNamePattern);
    }

    @Override
    public Struct getFilteredKey(PrimaryKeyMode primaryKeyMode, Set<String> primaryKeyFields, FieldNameFilter fieldsFilter) {
        if (primaryKeyMode == PrimaryKeyMode.RECORD_KEY && keySchema() != null && keySchema().type().isPrimitive()) {
            final Schema wrapper = SchemaBuilder.struct()
                    .name("io.debezium.connector.elasticsearch.PrimitiveKey")
                    .field(PRIMITIVE_KEY_FIELD, keySchema())
                    .build();
            return new Struct(wrapper).put(PRIMITIVE_KEY_FIELD, key());
        }
        return super.getFilteredKey(primaryKeyMode, primaryKeyFields, fieldsFilter);
    }
}
