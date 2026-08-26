/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * The resolved intent of one sink record after the input contract has been applied: a document
 * write, a document delete, a truncate of the resolved resource, or a deliberate skip.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 2"
 */
public sealed interface SinkOperation {

    DebeziumSinkRecord record();

    /**
     * A document write. {@code payload} is the document body before conversion; a schemaless
     * record carries the raw value in {@code rawValue} instead. {@code operation} is the envelope
     * operation where one exists, used for {@code write.method.per.operation} resolution.
     */
    record Write(DebeziumSinkRecord record, Envelope.Operation operation, Struct payload, Object rawValue) implements SinkOperation {

        public static Write of(DebeziumSinkRecord record, Envelope.Operation operation, Struct payload) {
            return new Write(record, operation, payload, null);
        }

        public static Write ofRaw(DebeziumSinkRecord record, Object rawValue) {
            return new Write(record, null, null, rawValue);
        }
    }

    /**
     * A delete by {@code _id}, derived from the record per {@code primary.key.mode}.
     */
    record Delete(DebeziumSinkRecord record) implements SinkOperation {
    }

    /**
     * A truncate ({@code t}) event; handled per {@code truncate.mode} with the DDD-61 5.1 sequencing.
     */
    record Truncate(DebeziumSinkRecord record) implements SinkOperation {
    }

    /**
     * A deliberately skipped record, counted in metrics with the reason retained for logging.
     */
    record Skip(DebeziumSinkRecord record, String reason) implements SinkOperation {
    }
}
