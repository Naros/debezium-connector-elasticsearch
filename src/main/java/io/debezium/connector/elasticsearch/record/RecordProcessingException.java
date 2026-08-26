/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.record;

import io.debezium.DebeziumException;

/**
 * A record-level processing failure (unusable id, missing routing field, contract violation)
 * that is routed to the error reporter rather than failing the task.
 *
 * @author Chris Cranford
 */
public class RecordProcessingException extends DebeziumException {

    public RecordProcessingException(String message) {
        super(message);
    }

    public RecordProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
