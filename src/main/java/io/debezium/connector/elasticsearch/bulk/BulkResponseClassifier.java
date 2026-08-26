/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import java.io.IOException;
import java.util.Map;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;

/**
 * Classifies every bulk item outcome and transport failure into an {@link ErrorBucket}. The
 * classification is a table keyed on HTTP status plus the Elasticsearch error {@code type}, and
 * {@code error.classification.overrides} moves a specific error type between buckets without a
 * release. The default for an unmatched outcome is {@link ErrorBucket#TASK_FATAL}, never
 * {@link ErrorBucket#TRANSIENT}: no unclassified condition is well enough understood to be
 * assumed recoverable.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 9.1"
 */
public class BulkResponseClassifier {

    private static final Map<String, ErrorBucket> BY_ERROR_TYPE = Map.ofEntries(
            Map.entry("es_rejected_execution_exception", ErrorBucket.TRANSIENT),
            Map.entry("unavailable_shards_exception", ErrorBucket.TRANSIENT),
            Map.entry("node_not_connected_exception", ErrorBucket.TRANSIENT),
            Map.entry("node_disconnected_exception", ErrorBucket.TRANSIENT),
            Map.entry("circuit_breaking_exception", ErrorBucket.TRANSIENT),
            Map.entry("no_shard_available_action_exception", ErrorBucket.TRANSIENT),
            Map.entry("process_cluster_event_timeout_exception", ErrorBucket.TRANSIENT),
            Map.entry("mapper_parsing_exception", ErrorBucket.RECORD),
            Map.entry("document_parsing_exception", ErrorBucket.RECORD),
            Map.entry("strict_dynamic_mapping_exception", ErrorBucket.RECORD),
            Map.entry("illegal_argument_exception", ErrorBucket.RECORD),
            Map.entry("version_conflict_engine_exception", ErrorBucket.RECORD),
            Map.entry("document_missing_exception", ErrorBucket.RECORD),
            Map.entry("routing_missing_exception", ErrorBucket.RECORD),
            Map.entry("cluster_block_exception", ErrorBucket.RESOURCE_FATAL),
            Map.entry("index_closed_exception", ErrorBucket.RESOURCE_FATAL),
            Map.entry("index_not_found_exception", ErrorBucket.RESOURCE_FATAL));

    private final Map<String, ErrorBucket> overrides;

    public BulkResponseClassifier(Map<String, ErrorBucket> overrides) {
        this.overrides = Map.copyOf(overrides);
    }

    /**
     * Classifies one bulk item response.
     */
    public ErrorBucket classify(BulkResponseItem item) {
        // A delete of an absent document reports status 404 with no error in current versions,
        // but older shapes carry a result of "not_found"; both are successes for a delete.
        if (item.error() == null) {
            return ErrorBucket.SUCCESS;
        }
        final String type = item.error().type();
        if (type != null) {
            final ErrorBucket override = overrides.get(type);
            if (override != null) {
                return override;
            }
            final ErrorBucket known = BY_ERROR_TYPE.get(type);
            if (known != null) {
                return known;
            }
            if ("security_exception".equals(type)) {
                // A 403 naming a single index is resource-scoped; a 401, or a 403 that is not, is not.
                return item.status() == 403 && item.index() != null ? ErrorBucket.RESOURCE_FATAL : ErrorBucket.TASK_FATAL;
            }
        }
        return switch (item.status()) {
            case 429, 503 -> ErrorBucket.TRANSIENT;
            case 400, 409 -> ErrorBucket.RECORD;
            default -> ErrorBucket.TASK_FATAL;
        };
    }

    /**
     * Classifies a transport-level failure of the whole bulk request.
     */
    public ErrorBucket classify(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof IOException) {
                return ErrorBucket.TRANSIENT;
            }
        }
        return ErrorBucket.TASK_FATAL;
    }
}
