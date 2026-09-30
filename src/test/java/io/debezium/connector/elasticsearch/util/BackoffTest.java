/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.util.DelayStrategy;

/**
 * Unit tests for {@link Backoff}: the configurations {@link DelayStrategy} would reject are
 * mapped to a strategy that still honors the intent.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class BackoffTest {

    @Test
    void shouldNeverSleepWithoutAnInitialDelay() {
        final DelayStrategy backoff = Backoff.exponential(0, 1000);

        assertThat(backoff.sleepWhen(true)).isFalse();
        assertThat(backoff.sleepWhen(true)).isFalse();
    }

    @Test
    void shouldSleepTheInitialDelayWhenTheMaximumDoesNotExceedIt() {
        final DelayStrategy backoff = Backoff.exponential(2, 2);

        final long start = System.nanoTime();
        assertThat(backoff.sleepWhen(true)).isTrue();
        assertThat(backoff.sleepWhen(true)).isTrue();
        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(4_000_000L);
    }

    @Test
    void shouldBackOffExponentiallyAndResetOnProgress() {
        final DelayStrategy backoff = Backoff.exponential(1, 4);

        assertThat(backoff.sleepWhen(true)).isTrue();
        assertThat(backoff.sleepWhen(true)).isTrue();
        assertThat(backoff.sleepWhen(false)).as("progress resets without sleeping").isFalse();
        assertThat(backoff.sleepWhen(true)).isTrue();
    }
}
