/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * Derives the Elasticsearch {@code _id} from a sink record through the shared
 * {@code primary.key.mode} and {@code primary.key.fields} properties. Composite fields join with
 * {@code document.id.separator} in the order given by {@code primary.key.fields}, or in schema
 * field order when that is unset; the order is fixed because a reordering silently changes every
 * {@code _id} in the index. An id longer than Elasticsearch's 512-byte limit, or a null id under
 * any mode other than {@code none}, is a record-level error.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 3"
 */
public class DocumentIdStrategy {

    private static final int MAX_ID_BYTES = 512;

    private final PrimaryKeyMode mode;
    private final Set<String> keyFields;
    private final String separator;

    public DocumentIdStrategy(ElasticsearchSinkConnectorConfig config) {
        this.mode = config.getPrimaryKeyMode();
        this.keyFields = config.getPrimaryKeyFields();
        this.separator = config.documentIdSeparator();
    }

    /**
     * Returns the document id, or empty under {@code primary.key.mode=none} where Elasticsearch
     * generates one.
     *
     * @throws RecordProcessingException on a null or oversized id
     */
    public Optional<String> documentId(DebeziumSinkRecord record) {
        final String id = switch (mode) {
            case NONE -> null;
            case KAFKA -> record.topicName() + separator + record.partition() + separator + record.offset();
            case RECORD_KEY -> fromRecordKey(record);
            case RECORD_VALUE -> fromStruct(record, record.getPayload(), "record value");
            case RECORD_HEADER -> fromStruct(record, headerStruct(record), "record headers");
        };
        if (mode == PrimaryKeyMode.NONE) {
            return Optional.empty();
        }
        if (id == null) {
            throw new RecordProcessingException(String.format(
                    "The document id resolved to null under '%s=%s' for record at topic '%s' partition %s offset %s.",
                    ElasticsearchSinkConnectorConfig.PRIMARY_KEY_MODE, mode.getValue(),
                    record.topicName(), record.partition(), record.offset()));
        }
        final int length = id.getBytes(StandardCharsets.UTF_8).length;
        if (length > MAX_ID_BYTES) {
            throw new RecordProcessingException(String.format(
                    "The document id for record at topic '%s' partition %s offset %s is %d bytes, exceeding Elasticsearch's "
                            + "%d-byte limit.",
                    record.topicName(), record.partition(), record.offset(), length, MAX_ID_BYTES));
        }
        return Optional.of(id);
    }

    private String fromRecordKey(DebeziumSinkRecord record) {
        final Object key = record.key();
        if (key == null) {
            return null;
        }
        final Schema keySchema = record.keySchema();
        if (key instanceof Struct structKey) {
            final List<String> ordered = keyFields.isEmpty()
                    ? structKey.schema().fields().stream().map(Field::name).toList()
                    : List.copyOf(keyFields);
            return joinFields(record, structKey, ordered, "record key");
        }
        if (keySchema != null && !keySchema.type().isPrimitive()) {
            throw new RecordProcessingException(String.format(
                    "Unsupported record key schema type '%s' for record at topic '%s' partition %s offset %s.",
                    keySchema.type(), record.topicName(), record.partition(), record.offset()));
        }
        return stringify(key);
    }

    private String fromStruct(DebeziumSinkRecord record, Struct struct, String origin) {
        if (struct == null) {
            throw new RecordProcessingException(String.format(
                    "'%s=%s' but the %s is absent for record at topic '%s' partition %s offset %s.",
                    ElasticsearchSinkConnectorConfig.PRIMARY_KEY_MODE, mode.getValue(), origin,
                    record.topicName(), record.partition(), record.offset()));
        }
        return joinFields(record, struct, List.copyOf(keyFields), origin);
    }

    private String joinFields(DebeziumSinkRecord record, Struct struct, List<String> fields, String origin) {
        final List<String> parts = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (struct.schema().field(field) == null) {
                throw new RecordProcessingException(String.format(
                        "Document id field '%s' does not exist in the %s for record at topic '%s' partition %s offset %s.",
                        field, origin, record.topicName(), record.partition(), record.offset()));
            }
            final Object value = struct.get(field);
            if (value == null) {
                throw new RecordProcessingException(String.format(
                        "Document id field '%s' is null in the %s for record at topic '%s' partition %s offset %s.",
                        field, origin, record.topicName(), record.partition(), record.offset()));
            }
            parts.add(stringify(value));
        }
        return parts.isEmpty() ? null : String.join(separator, parts);
    }

    private Struct headerStruct(DebeziumSinkRecord record) {
        return record instanceof KafkaDebeziumSinkRecord kafkaRecord ? kafkaRecord.kafkaHeader() : null;
    }

    private String stringify(Object value) {
        if (value instanceof ByteBuffer buffer) {
            return Base64.getEncoder().encodeToString(toBytes(buffer));
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return value.toString();
    }

    private byte[] toBytes(ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
