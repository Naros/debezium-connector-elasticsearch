/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

/**
 * The pause between retries, injectable so backoff and stall handling can be tested against a
 * fake clock rather than wall time.
 *
 * @author Chris Cranford
 */
@FunctionalInterface
public interface Sleeper {

    /**
     * Pauses the calling thread for the given duration.
     */
    void sleep(long millis) throws InterruptedException;

    /**
     * The production sleeper, backed by {@link Thread#sleep(long)}.
     */
    static Sleeper system() {
        return Thread::sleep;
    }
}
