/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import io.debezium.config.EnumeratedValue;

/**
 * Classification buckets for bulk item outcomes and transport failures.
 * <p>
 * {@link #SUCCESS} is deliberately not assignable through {@code error.classification.overrides}:
 * reclassifying a failure as a success is how a document is lost silently.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 9.1"
 */
public enum ErrorBucket implements EnumeratedValue {

    /** The item was applied; includes {@code noop} and {@code not_found} on a delete. */
    SUCCESS("success"),

    /** A backpressure or availability signal; throttle and retry, never counted as failure. */
    TRANSIENT("transient"),

    /** A permanent per-record failure; route to the error reporter. */
    RECORD("record"),

    /** The target resource is unusable (read-only, closed, denied) while others may not be. */
    RESOURCE_FATAL("resource_fatal"),

    /** The task cannot make progress at all; fail fast with a diagnostic message. */
    TASK_FATAL("task_fatal");

    private final String value;

    ErrorBucket(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * Parses an override bucket name from {@code error.classification.overrides}.
     *
     * @param value the configured bucket name
     * @return the bucket, or {@code null} when the name is unknown or names {@code success}
     */
    public static ErrorBucket parseOverride(String value) {
        final ErrorBucket bucket = EnumeratedValue.parse(ErrorBucket.class, value);
        return bucket == SUCCESS ? null : bucket;
    }
}
