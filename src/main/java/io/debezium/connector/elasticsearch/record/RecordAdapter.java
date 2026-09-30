/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import org.apache.kafka.connect.data.Struct;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TombstoneMode;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * Maps each sink record onto a {@link SinkOperation} per the supported input contracts: the
 * Debezium envelope, the flattened Debezium record (ExtractNewRecordState applied, markers in
 * value fields or headers), and the plain structured event, resolved per record under
 * {@code event.format=auto} or forced by the explicit modes.
 * <p>
 * {@code tombstone.mode=auto} resolves to {@code delete} on every path: the deduplicating
 * buffer collapses a {@code d} event and its tombstone into just the tombstone, so ignoring the
 * tombstone would lose the delete outright, while the redundant delete it produces instead is
 * benign. An explicit {@code ignore} remains available.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 2"
 * @see "DDD-61 Section 2.1"
 */
public class RecordAdapter {

    private static final String FLATTENED_DELETED_FIELD = "__deleted";
    private static final String FLATTENED_OP_FIELD = "__op";

    private final ElasticsearchSinkConnectorConfig config;

    public RecordAdapter(ElasticsearchSinkConnectorConfig config) {
        this.config = config;
    }

    /**
     * Resolves the operation for the given record.
     *
     * @throws RecordProcessingException on a record-level contract violation, to be routed to
     *         the error reporter
     */
    public SinkOperation adapt(DebeziumSinkRecord record) {
        return switch (config.eventFormat()) {
            case PLAIN -> adaptPlain(record);
            case DEBEZIUM -> adaptDebezium(record, true);
            case AUTO -> adaptDebezium(record, false);
        };
    }

    private SinkOperation adaptDebezium(DebeziumSinkRecord record, boolean required) {
        if (record.isTombstone()) {
            return adaptTombstone(record);
        }
        if (record.isDebeziumMessage()) {
            return adaptEnvelope(record);
        }
        if (isFlattenedDebezium(record)) {
            return adaptFlattened(record);
        }
        if (required) {
            throw new RecordProcessingException(String.format(
                    "'%s' is 'debezium' but the record at topic '%s' partition %s offset %s is neither a Debezium envelope "
                            + "nor a flattened Debezium record.",
                    ElasticsearchSinkConnectorConfig.EVENT_FORMAT, record.topicName(), record.partition(), record.offset()));
        }
        return adaptPlain(record);
    }

    private SinkOperation adaptEnvelope(DebeziumSinkRecord record) {
        final Struct envelope = (Struct) record.value();
        final Envelope.Operation operation = Envelope.Operation.forCode(envelope.getString(Envelope.FieldName.OPERATION));
        if (operation == null) {
            throw new RecordProcessingException(String.format(
                    "Unknown envelope operation '%s' at topic '%s' partition %s offset %s.",
                    envelope.getString(Envelope.FieldName.OPERATION), record.topicName(), record.partition(), record.offset()));
        }
        return switch (operation) {
            case CREATE, READ, UPDATE -> SinkOperation.Write.of(record, operation, record.getPayload());
            case DELETE -> new SinkOperation.Delete(record);
            case TRUNCATE -> new SinkOperation.Truncate(record);
            case MESSAGE -> new SinkOperation.Skip(record, "logical message ('m') event");
        };
    }

    private SinkOperation adaptFlattened(DebeziumSinkRecord record) {
        if (isNullValueWithSchema(record)) {
            // ExtractNewRecordState with delete.handling.mode=none: a null value with a schema is
            // the delete signal on this path.
            return new SinkOperation.Delete(record);
        }
        final Struct payload = record.getPayload();
        if (isFlattenedDelete(record, payload)) {
            return new SinkOperation.Delete(record);
        }
        return SinkOperation.Write.of(record, flattenedOperation(record, payload), payload);
    }

    private SinkOperation adaptPlain(DebeziumSinkRecord record) {
        if (record.value() == null) {
            return adaptTombstone(record);
        }
        if (record.value() instanceof Struct) {
            return SinkOperation.Write.of(record, null, record.getPayload());
        }
        return SinkOperation.Write.ofRaw(record, record.value());
    }

    /**
     * Whether the Kafka value is null while a value schema is present: ExtractNewRecordState with
     * {@code delete.handling.mode=none}. The shared binding substitutes an empty struct for such a
     * value, so the original record is consulted.
     */
    private boolean isNullValueWithSchema(DebeziumSinkRecord record) {
        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
            final org.apache.kafka.connect.sink.SinkRecord original = kafkaRecord.getOriginalKafkaRecord();
            return original.value() == null && original.valueSchema() != null;
        }
        return record.value() == null && record.valueSchema() != null;
    }

    private SinkOperation adaptTombstone(DebeziumSinkRecord record) {
        final TombstoneMode mode = config.tombstoneMode() == TombstoneMode.AUTO ? TombstoneMode.DELETE : config.tombstoneMode();
        if (record.key() == null) {
            // With no key there is nothing to act on; fail by default regardless of mode.
            if (mode == TombstoneMode.IGNORE) {
                return new SinkOperation.Skip(record, "tombstone with null key");
            }
            throw new RecordProcessingException(String.format(
                    "Tombstone with a null key at topic '%s' partition %s offset %s: there is nothing to act on.",
                    record.topicName(), record.partition(), record.offset()));
        }
        return switch (mode) {
            case IGNORE -> new SinkOperation.Skip(record, "tombstone (mode 'ignore')");
            case DELETE -> new SinkOperation.Delete(record);
            case FAIL -> throw new RecordProcessingException(String.format(
                    "Tombstone at topic '%s' partition %s offset %s and '%s' is 'fail'.",
                    record.topicName(), record.partition(), record.offset(), ElasticsearchSinkConnectorConfig.TOMBSTONE_MODE));
            case AUTO -> throw new IllegalStateException("'auto' is resolved to 'delete' before this switch");
        };
    }

    private boolean isFlattenedDebezium(DebeziumSinkRecord record) {
        if (isNullValueWithSchema(record)) {
            return true;
        }
        if (!(record instanceof KafkaDebeziumSinkRecord kafkaRecord) || !kafkaRecord.isFlattened()) {
            return false;
        }
        final Struct payload = record.getPayload();
        if (payload != null && payload.schema() != null
                && (payload.schema().field(FLATTENED_OP_FIELD) != null || payload.schema().field(FLATTENED_DELETED_FIELD) != null)) {
            return true;
        }
        return hasHeader(record, FLATTENED_OP_FIELD) || hasHeader(record, FLATTENED_DELETED_FIELD);
    }

    private boolean isFlattenedDelete(DebeziumSinkRecord record, Struct payload) {
        final Object deleted = flattenedMarker(record, payload, FLATTENED_DELETED_FIELD);
        if (deleted != null) {
            return Boolean.parseBoolean(deleted.toString());
        }
        final Object op = flattenedMarker(record, payload, FLATTENED_OP_FIELD);
        return op != null && Envelope.Operation.DELETE == Envelope.Operation.forCode(op.toString());
    }

    private Envelope.Operation flattenedOperation(DebeziumSinkRecord record, Struct payload) {
        final Object op = flattenedMarker(record, payload, FLATTENED_OP_FIELD);
        return op != null ? Envelope.Operation.forCode(op.toString()) : null;
    }

    /**
     * Reads an ExtractNewRecordState marker from the value fields ({@code add.fields}) or, when
     * absent there, from the record headers ({@code add.headers}).
     */
    private Object flattenedMarker(DebeziumSinkRecord record, Struct payload, String name) {
        if (payload != null && payload.schema() != null && payload.schema().field(name) != null) {
            return payload.get(name);
        }
        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
            final Struct headers = kafkaRecord.kafkaHeader();
            if (headers != null && headers.schema().field(name) != null) {
                return headers.get(name);
            }
        }
        return null;
    }

    private boolean hasHeader(DebeziumSinkRecord record, String name) {
        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
            final Struct headers = kafkaRecord.kafkaHeader();
            return headers != null && headers.schema().field(name) != null;
        }
        return false;
    }
}
