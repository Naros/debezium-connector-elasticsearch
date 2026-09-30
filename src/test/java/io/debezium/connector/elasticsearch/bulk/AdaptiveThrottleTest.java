/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AdaptiveThrottle}: the reactive halving and additive recovery of
 * DDD-61 section 8, and the optional proactive ceilings.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class AdaptiveThrottleTest {

    @Test
    void shouldStartAtConfiguredBatchSize() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(500, null, null);
        assertThat(throttle.effectiveBatchSize()).isEqualTo(500);
        assertThat(throttle.throttleEvents()).isZero();
    }

    @Test
    void shouldHalveOnRejectionDownToOne() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(10, null, null);
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(5);
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(2);
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(1);
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(1);
        assertThat(throttle.throttleEvents()).isEqualTo(4);
    }

    @Test
    void shouldRecoverAdditivelyAfterThreeCleanResponses() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, null, null);
        throttle.onRejection();
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(25);

        throttle.onSuccess();
        throttle.onSuccess();
        assertThat(throttle.effectiveBatchSize()).as("two clean responses are not enough").isEqualTo(25);
        throttle.onSuccess();
        assertThat(throttle.effectiveBatchSize()).as("one tenth of the configured size per step").isEqualTo(35);

        throttle.onSuccess();
        throttle.onSuccess();
        throttle.onSuccess();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(45);
    }

    @Test
    void shouldResetCleanResponseCountOnRejection() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, null, null);
        throttle.onRejection();
        throttle.onSuccess();
        throttle.onSuccess();
        throttle.onRejection();
        throttle.onSuccess();
        assertThat(throttle.effectiveBatchSize()).as("the streak restarted after the second rejection").isEqualTo(25);
    }

    @Test
    void shouldCapRecoveryAtConfiguredBatchSizeAndStepByAtLeastOne() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(5, null, null);
        throttle.onRejection();
        throttle.onRejection();
        assertThat(throttle.effectiveBatchSize()).isEqualTo(1);
        for (int i = 0; i < 3 * 10; i++) {
            throttle.onSuccess();
        }
        assertThat(throttle.effectiveBatchSize()).isEqualTo(5);
        // Below ten records the additive step is one, so recovery takes one round per record.
        final AdaptiveThrottle small = new AdaptiveThrottle(4, null, null);
        small.onRejection();
        small.onSuccess();
        small.onSuccess();
        small.onSuccess();
        assertThat(small.effectiveBatchSize()).isEqualTo(3);
    }

    @Test
    void shouldReturnImmediatelyWithoutCeilings() throws InterruptedException {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, null, null);
        final long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            throttle.awaitPermit(1_000_000);
        }
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void shouldPaceRequestsByMaxRequestsPerSecond() throws InterruptedException {
        // 20 requests per second is one every 50 ms; the first permit is free, the next three wait.
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, 20.0, null);
        final long start = System.nanoTime();
        for (int i = 0; i < 4; i++) {
            throttle.awaitPermit(0);
        }
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(140));
    }

    @Test
    void shouldPaceRequestsByMaxBytesPerSecond() throws InterruptedException {
        // 10 KiB per second: a 1 KiB request costs 100 ms; three requests wait for two of them.
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, null, 10_240L);
        final long start = System.nanoTime();
        for (int i = 0; i < 3; i++) {
            throttle.awaitPermit(1_024);
        }
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(180));
    }

    @Test
    void shouldNotChargeZeroByteRequestsAgainstByteCeiling() throws InterruptedException {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(100, null, 1L);
        final long start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            throttle.awaitPermit(0);
        }
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(1));
    }
}
