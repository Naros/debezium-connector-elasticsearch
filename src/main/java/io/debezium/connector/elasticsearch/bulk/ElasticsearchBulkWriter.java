/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.client.ClusterVersionHandshake;
import io.debezium.connector.elasticsearch.metrics.ElasticsearchSinkConnectorMetrics;
import io.debezium.connector.elasticsearch.util.Sleeper;
import io.debezium.dlq.ErrorReporter;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.util.Clock;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

/**
 * Executes bulk requests with exactly one request in flight per task, over batches already
 * reduced to one write per resource and {@code _id}, so a retried item can never be overtaken
 * by a sibling for the same document. Rejections (429) are throttled and never counted against
 * the retry budget, record-level failures route to the error reporter with the Elasticsearch
 * error attached, a blocked resource stops only itself until {@code progress.stall.timeout.ms}
 * elapses, and an unmatched outcome fails the task.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 7.2"
 * @see "DDD-61 Section 9.1"
 */
public class ElasticsearchBulkWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchBulkWriter.class);

    /**
     * One prepared bulk item: the source record it came from, the immutable operation to issue,
     * and bookkeeping for byte-bound chunking and metrics.
     */
    public record BulkItem(BatchRecord source, BulkOperation operation, boolean delete, long approximateBytes) {

        String resource() {
            return source.collectionId().name();
        }

        DebeziumSinkRecord record() {
            return source.record();
        }
    }

    private final ElasticsearchClient client;
    private final ElasticsearchSinkConnectorConfig config;
    private final BulkResponseClassifier classifier;
    private final AdaptiveThrottle throttle;
    private final ErrorReporter errorReporter;
    private final ElasticsearchSinkConnectorMetrics metrics;
    private final ClusterVersionHandshake handshake;
    private final Clock clock;
    private final Sleeper sleeper;

    private long lastProgressMs;
    private Exception lastFailure;

    public ElasticsearchBulkWriter(ElasticsearchClient client, ElasticsearchSinkConnectorConfig config,
                                   AdaptiveThrottle throttle, ErrorReporter errorReporter,
                                   ElasticsearchSinkConnectorMetrics metrics, ClusterVersionHandshake handshake) {
        this(client, config, throttle, errorReporter, metrics, handshake, Clock.system(), Sleeper.system());
    }

    /**
     * The clock and sleeper govern backoff and {@code progress.stall.timeout.ms}; injectable so
     * those paths are testable without wall time.
     */
    public ElasticsearchBulkWriter(ElasticsearchClient client, ElasticsearchSinkConnectorConfig config,
                                   AdaptiveThrottle throttle, ErrorReporter errorReporter,
                                   ElasticsearchSinkConnectorMetrics metrics, ClusterVersionHandshake handshake,
                                   Clock clock, Sleeper sleeper) {
        this.client = client;
        this.config = config;
        this.classifier = new BulkResponseClassifier(config.errorClassificationOverrides());
        this.throttle = throttle;
        this.errorReporter = errorReporter;
        this.metrics = metrics;
        this.handshake = handshake;
        this.clock = clock;
        this.sleeper = sleeper;
        this.lastProgressMs = clock.currentTimeInMillis();
    }

    /**
     * Writes all items, blocking until every one has been applied, routed to the error reporter,
     * or the task has been declared failed. Returns normally only when the batch is fully resolved.
     */
    public void write(List<BulkItem> items) {
        List<BulkItem> pending = new ArrayList<>(items);
        final Set<String> batchResources = new HashSet<>();
        items.forEach(item -> batchResources.add(item.resource()));
        final Map<String, Long> blockedSince = new HashMap<>();
        int budgetedRetries = 0;
        long backoffMs = config.retryBackoffMs();
        lastProgressMs = clock.currentTimeInMillis();

        while (!pending.isEmpty()) {
            // The chunk is a prefix of the pending list; unprocessed items stay pending in order.
            final List<BulkItem> chunk = nextChunk(pending);
            final List<BulkItem> rest = List.copyOf(pending.subList(chunk.size(), pending.size()));
            final RoundResult round = executeRound(chunk, blockedSince);

            pending = new ArrayList<>(round.retryable());
            pending.addAll(round.blocked());
            pending.addAll(rest);

            if (round.progressed()) {
                lastProgressMs = clock.currentTimeInMillis();
                budgetedRetries = 0;
                backoffMs = config.retryBackoffMs();
                metrics.bulkRequestCompleted();
            }
            metrics.effectiveBatchSize(throttle.effectiveBatchSize());
            metrics.throttled(throttle.throttleEvents());
            metrics.blockedResources(blockedSince.size());

            if (pending.isEmpty()) {
                break;
            }

            enforceStallTimeout(pending, batchResources, blockedSince);

            if (!round.progressed()) {
                if (round.budgeted()) {
                    budgetedRetries++;
                    metrics.retried();
                    if (budgetedRetries > config.maxRetries()) {
                        throw new ConnectException(String.format(
                                "Bulk writes failed %d consecutive times and 'max.retries' is exhausted; last error: %s",
                                budgetedRetries, describe(lastFailure)), lastFailure);
                    }
                }
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, config.retryBackoffMaxMs());
            }
        }
    }

    /**
     * The outcome of one request. A round is {@code budgeted} when it failed for a reason the
     * retry budget exists for: a transient non-throttle item failure or a transport failure.
     * Rejections (429) are backpressure and blocked resources are bounded by the stall timeout,
     * so neither spends 'max.retries'.
     */
    private record RoundResult(List<BulkItem> retryable, List<BulkItem> blocked, boolean progressed, boolean budgeted) {
    }

    private RoundResult executeRound(List<BulkItem> chunk, Map<String, Long> blockedSince) {
        final BulkResponse response;
        try {
            throttle.awaitPermit(chunk.stream().mapToLong(BulkItem::approximateBytes).sum());
            response = client.bulk(BulkRequest.of(b -> b.operations(chunk.stream().map(BulkItem::operation).toList())));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while writing to Elasticsearch", e);
        }
        catch (Exception e) {
            return classifyTransportFailure(chunk, e);
        }

        final List<BulkItem> retryable = new ArrayList<>();
        final List<BulkItem> blocked = new ArrayList<>();
        boolean progressed = false;
        boolean sawRejection = false;
        boolean sawNonThrottleFailure = false;

        final List<BulkResponseItem> results = response.items();
        if (results.size() != chunk.size()) {
            // Fewer results than operations would silently drop the unanswered items as done;
            // more is a response for some other request. Neither is a shape to recover from.
            throw new ConnectException(String.format(
                    "Bulk response carried %d item results for %d operations; failing the task rather than guessing which "
                            + "operations were applied.",
                    results.size(), chunk.size()));
        }
        for (int i = 0; i < results.size(); i++) {
            final BulkResponseItem result = results.get(i);
            final BulkItem item = chunk.get(i);
            switch (classifier.classify(result)) {
                case SUCCESS -> {
                    progressed = true;
                    blockedSince.remove(item.resource());
                    if (item.delete()) {
                        metrics.deleted(1);
                    }
                    else {
                        metrics.written(1);
                    }
                    metrics.resourceWritten(item.resource());
                }
                case TRANSIENT -> {
                    lastFailure = itemFailure(item, result);
                    if (result.status() == 429) {
                        sawRejection = true;
                    }
                    else {
                        sawNonThrottleFailure = true;
                    }
                    retryable.add(item);
                }
                case RECORD -> {
                    progressed = true;
                    reportRecord(item, result);
                }
                case RESOURCE_FATAL -> {
                    lastFailure = itemFailure(item, result);
                    if (blockedSince.putIfAbsent(item.resource(), clock.currentTimeInMillis()) == null) {
                        LOGGER.warn("Resource '{}' is blocked and will be retried until 'progress.stall.timeout.ms' elapses: {}",
                                item.resource(), describe(lastFailure));
                    }
                    blocked.add(item);
                }
                case TASK_FATAL -> throw new ConnectException(String.format(
                        "Bulk item for resource '%s' failed fatally: %s", item.resource(), describeItem(result)));
            }
        }

        if (sawRejection) {
            throttle.onRejection();
        }
        else {
            throttle.onSuccess();
        }
        return new RoundResult(retryable, blocked, progressed, sawNonThrottleFailure);
    }

    private RoundResult classifyTransportFailure(List<BulkItem> chunk, Exception failure) {
        lastFailure = failure;
        if (isVersionShaped(failure)) {
            // A media-type or compatibility rejection invalidates the cached cluster version and
            // forces a re-probe before the next request (DDD-61 10.1.1); covers a cluster
            // upgraded underneath a long-running task.
            LOGGER.warn("A compatibility-shaped transport error occurred; re-probing the cluster version.", failure);
            handshake.invalidate();
            handshake.ensureProbed();
            return new RoundResult(chunk, List.of(), false, true);
        }
        return switch (classifier.classify(failure)) {
            case TRANSIENT -> {
                LOGGER.warn("Transient transport failure writing bulk request: {}", describe(failure));
                yield new RoundResult(chunk, List.of(), false, true);
            }
            // No unclassified condition is well enough understood to be assumed recoverable.
            default -> throw new ConnectException("Bulk request failed with an unclassified error; failing the task rather "
                    + "than retrying forever. Use 'error.classification.overrides' to reclassify if this error is known to be "
                    + "recoverable: " + describe(failure), failure);
        };
    }

    private void enforceStallTimeout(List<BulkItem> pending, Set<String> batchResources, Map<String, Long> blockedSince) {
        final long now = clock.currentTimeInMillis();
        if (now - lastProgressMs <= config.progressStallTimeoutMs()) {
            return;
        }
        // Blocked resources past the deadline are drained to the error reporter so offsets stay
        // sound. The task fails instead only when every resource in the batch is blocked, because
        // there is then no progress to protect (DDD-61 9.1).
        if (!blockedSince.isEmpty() && blockedSince.keySet().containsAll(batchResources)) {
            throw new ConnectException(String.format(
                    "No progress for longer than 'progress.stall.timeout.ms' (%d ms) and every resource in the batch %s is "
                            + "blocked; last error: %s",
                    config.progressStallTimeoutMs(), batchResources, describe(lastFailure)),
                    lastFailure);
        }
        final List<BulkItem> expired = pending.stream().filter(item -> blockedSince.containsKey(item.resource())).toList();
        if (!expired.isEmpty()) {
            expired.forEach(item -> {
                reportRecord(item, new DebeziumException(String.format(
                        "Resource '%s' remained blocked past 'progress.stall.timeout.ms': %s", item.resource(), describe(lastFailure))));
                blockedSince.remove(item.resource());
            });
            pending.removeAll(expired);
            lastProgressMs = clock.currentTimeInMillis();
            return;
        }
        throw new ConnectException(String.format(
                "No progress for longer than 'progress.stall.timeout.ms' (%d ms); last error: %s",
                config.progressStallTimeoutMs(), describe(lastFailure)), lastFailure);
    }

    private List<BulkItem> nextChunk(List<BulkItem> pending) {
        final int maxItems = Math.min(throttle.effectiveBatchSize(), pending.size());
        final long maxBytes = config.bulkSizeBytes();
        final List<BulkItem> chunk = new ArrayList<>(maxItems);
        long bytes = 0;
        for (BulkItem item : pending) {
            if (chunk.size() >= maxItems) {
                break;
            }
            if (maxBytes > 0 && !chunk.isEmpty() && bytes + item.approximateBytes() > maxBytes) {
                break;
            }
            chunk.add(item);
            bytes += item.approximateBytes();
        }
        return chunk;
    }

    private void reportRecord(BulkItem item, BulkResponseItem result) {
        reportRecord(item, itemFailure(item, result));
    }

    private void reportRecord(BulkItem item, Exception failure) {
        final DebeziumSinkRecord record = item.record();
        LOGGER.warn("Routing record from topic '{}' partition {} offset {} to the error reporter: {}",
                record.topicName(), record.partition(), record.offset(), describe(failure));
        errorReporter.report(record, failure);
        metrics.errantRecordsReported(1);
    }

    private DebeziumException itemFailure(BulkItem item, BulkResponseItem result) {
        return new DebeziumException(String.format(
                "Bulk %s for index '%s'%s failed with status %d: %s",
                result.operationType(), item.resource(),
                config.isLogSensitiveData() ? " id '" + result.id() + "'" : "",
                result.status(), describeItem(result)));
    }

    private String describeItem(BulkResponseItem result) {
        return String.format("[%s] %s", result.error().type(), result.error().reason());
    }

    private boolean isVersionShaped(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof ResponseException response) {
                final int status = response.getResponse().getStatusLine().getStatusCode();
                if (status == 406 || status == 415) {
                    return true;
                }
            }
            final String message = current.getMessage();
            if (message != null && (message.contains("media_type_header_exception") || message.contains("Content-Type"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String describe(Throwable failure) {
        return failure == null ? "none" : failure.getMessage();
    }

    private void sleep(long millis) {
        try {
            sleeper.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while backing off between bulk retries", e);
        }
    }
}
