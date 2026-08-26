/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reactive backpressure control plus the optional proactive rate ceilings.
 * <p>
 * A 429 rejection halves the effective batch size; a configurable number of clean responses
 * recovers it additively toward the configured batch size. Throttle waits are backpressure
 * handling, never counted against the retry budget. The optional request and byte ceilings
 * compose with the throttle: the effective rate is the lower of the two.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 8"
 */
public class AdaptiveThrottle {

    private static final int RECOVERY_THRESHOLD = 3;

    private final int configuredBatchSize;
    private final Double maxRequestsPerSecond;
    private final Long maxBytesPerSecond;

    private final AtomicInteger effectiveBatchSize;
    private final AtomicInteger cleanResponses = new AtomicInteger();
    private final AtomicLong throttleEvents = new AtomicLong();
    private final AtomicLong nextRequestNanos = new AtomicLong();

    public AdaptiveThrottle(int configuredBatchSize, Double maxRequestsPerSecond, Long maxBytesPerSecond) {
        this.configuredBatchSize = configuredBatchSize;
        this.effectiveBatchSize = new AtomicInteger(configuredBatchSize);
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    public int effectiveBatchSize() {
        return effectiveBatchSize.get();
    }

    public long throttleEvents() {
        return throttleEvents.get();
    }

    /**
     * Records a 429/rejection outcome: halves the effective batch size.
     */
    public void onRejection() {
        throttleEvents.incrementAndGet();
        cleanResponses.set(0);
        effectiveBatchSize.updateAndGet(size -> Math.max(1, size / 2));
    }

    /**
     * Records a clean bulk response: after {@value #RECOVERY_THRESHOLD} in a row, recovers the
     * effective batch size additively toward the configured value.
     */
    public void onSuccess() {
        if (cleanResponses.incrementAndGet() >= RECOVERY_THRESHOLD) {
            cleanResponses.set(0);
            effectiveBatchSize.updateAndGet(size -> Math.min(configuredBatchSize, size + Math.max(1, configuredBatchSize / 10)));
        }
    }

    /**
     * Blocks, when a proactive ceiling is configured, until the next request of the given size
     * may be issued.
     */
    public void awaitPermit(long requestBytes) throws InterruptedException {
        if (maxRequestsPerSecond == null && maxBytesPerSecond == null) {
            return;
        }
        long costNanos = 0;
        if (maxRequestsPerSecond != null) {
            costNanos = (long) (TimeUnit.SECONDS.toNanos(1) / maxRequestsPerSecond);
        }
        if (maxBytesPerSecond != null && requestBytes > 0) {
            costNanos = Math.max(costNanos, TimeUnit.SECONDS.toNanos(1) * requestBytes / maxBytesPerSecond);
        }

        while (true) {
            final long now = System.nanoTime();
            final long scheduled = nextRequestNanos.get();
            final long start = Math.max(now, scheduled);
            if (nextRequestNanos.compareAndSet(scheduled, start + costNanos)) {
                final long waitNanos = start - now;
                if (waitNanos > 0) {
                    TimeUnit.NANOSECONDS.sleep(waitNanos);
                }
                return;
            }
        }
    }
}
