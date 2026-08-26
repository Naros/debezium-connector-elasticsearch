/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import java.util.Locale;

/**
 * Classification buckets for bulk item outcomes and transport failures.
 * <p>
 * {@link #SUCCESS} is deliberately not assignable through {@code error.classification.overrides}:
 * reclassifying a failure as a success is how a document is lost silently.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 9.1"
 */
public enum ErrorBucket {

    /** The item was applied; includes {@code noop} and {@code not_found} on a delete. */
    SUCCESS,

    /** A backpressure or availability signal; throttle and retry, never counted as failure. */
    TRANSIENT,

    /** A permanent per-record failure; route to the error reporter. */
    RECORD,

    /** The target resource is unusable (read-only, closed, denied) while others may not be. */
    RESOURCE_FATAL,

    /** The task cannot make progress at all; fail fast with a diagnostic message. */
    TASK_FATAL;

    /**
     * Parses an override bucket name from {@code error.classification.overrides}.
     *
     * @param value the configured bucket name
     * @return the bucket, or {@code null} when the name is unknown or names {@code success}
     */
    public static ErrorBucket parseOverride(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "transient" -> TRANSIENT;
            case "record" -> RECORD;
            case "resource_fatal" -> RESOURCE_FATAL;
            case "task_fatal" -> TASK_FATAL;
            default -> null;
        };
    }
}
