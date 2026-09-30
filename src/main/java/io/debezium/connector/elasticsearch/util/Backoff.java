/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import java.time.Duration;

import io.debezium.util.DelayStrategy;

/**
 * Builds the retry backoff from {@code retry.backoff.ms} and {@code retry.backoff.max.ms} on
 * top of {@link DelayStrategy}, which insists on a positive initial delay and a strictly larger
 * maximum; the degenerate configurations map to no delay or a constant one.
 *
 * @author Chris Cranford
 */
public final class Backoff {

    private Backoff() {
    }

    /**
     * An exponential backoff that starts at {@code initialMs}, doubles, and caps at {@code maxMs}.
     * Zero or negative initial delay never sleeps; a maximum at or below the initial delay sleeps
     * the initial delay every time.
     */
    public static DelayStrategy exponential(long initialMs, long maxMs) {
        if (initialMs <= 0) {
            return DelayStrategy.none();
        }
        if (maxMs <= initialMs) {
            return DelayStrategy.constant(Duration.ofMillis(initialMs));
        }
        return DelayStrategy.exponential(Duration.ofMillis(initialMs), Duration.ofMillis(maxMs));
    }
}
