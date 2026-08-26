/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.naming;

import io.debezium.DebeziumException;

/**
 * A record-level resource naming failure: an unresolvable placeholder, or a resolved name that
 * breaks an Elasticsearch naming rule under {@code resource.name.invalid.handling=error_handler}.
 * Routed to the error reporter rather than failing the task.
 *
 * @author Chris Cranford
 */
public class ResourceResolutionException extends DebeziumException {

    public ResourceResolutionException(String message) {
        super(message);
    }
}
