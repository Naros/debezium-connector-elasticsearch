/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.metrics;

/**
 * JMX metrics for the Elasticsearch sink. These complement Kafka Connect's own sink-task
 * metrics, which stop at the {@code put()} boundary, by describing what the sink actually
 * applied to Elasticsearch and what the connection resolved to. The milliseconds since the last
 * successful bulk response is the observable behind progress-based task health, and the
 * resolved cluster version and compatibility mode are exposed because their invisibility is
 * what made compatibility failures hard to diagnose elsewhere.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 9.3"
 * @see "DDD-61 Section 10.1.1"
 * @see "DDD-61 Section 11"
 */
public interface ElasticsearchSinkConnectorMetricsMXBean {

    /**
     * @return the total number of documents written (indexed, created, updated, or upserted)
     */
    long getTotalNumberOfWrites();

    /**
     * @return the total number of document deletes applied, including deletes of absent documents
     */
    long getTotalNumberOfDeletes();

    /**
     * @return the total number of truncate operations applied
     */
    long getTotalNumberOfTruncates();

    /**
     * @return the number of records skipped without being applied (tombstones under 'ignore',
     *         logical message events, disabled deletes or truncates)
     */
    long getTotalNumberOfFilteredEvents();

    /**
     * @return the total number of records routed to the error reporter (dead letter queue)
     */
    long getTotalNumberOfErrantRecords();

    /**
     * @return the total number of bulk requests issued
     */
    long getTotalNumberOfBulkRequests();

    /**
     * @return the number of throttle events (429 rejections handled as backpressure)
     */
    long getTotalNumberOfThrottleEvents();

    /**
     * @return the number of retried bulk items counted against the retry budget
     */
    long getTotalNumberOfRetries();

    /**
     * @return the number of writes rejected as stale by external versioning
     */
    long getTotalNumberOfVersionConflicts();

    /**
     * @return the current effective batch size after adaptive throttling
     */
    int getEffectiveBatchSize();

    /**
     * @return milliseconds since the last successful bulk response, or -1 before the first
     */
    long getMillisSinceLastSuccessfulBulkResponse();

    /**
     * @return the number of resources currently blocked by a resource-fatal outcome
     */
    int getBlockedResourceCount();

    /**
     * @return the number of distinct resources written to since startup
     */
    int getDistinctResourceCount();

    /**
     * @return the detected Elasticsearch cluster version, or empty before the handshake
     */
    String getClusterVersion();

    /**
     * @return the effective REST API compatibility mode resolved at startup
     */
    String getApiCompatibilityMode();
}
