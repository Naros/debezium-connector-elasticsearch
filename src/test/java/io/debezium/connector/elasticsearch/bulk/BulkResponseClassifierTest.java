/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;

/**
 * Unit tests for {@link BulkResponseClassifier}: the DDD-61 section 9.1 classification table,
 * the status fallbacks, operator overrides, and transport failure classification.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class BulkResponseClassifierTest {

    private final BulkResponseClassifier classifier = new BulkResponseClassifier(Map.of());

    @Test
    void shouldClassifyItemWithoutErrorAsSuccess() {
        assertThat(classifier.classify(item(201, null))).isEqualTo(ErrorBucket.SUCCESS);
        // A delete of an absent document is a 404 with no error: a success for a delete.
        assertThat(classifier.classify(BulkResponseItem.of(b -> b.index("customers").status(404).operationType(OperationType.Delete))))
                .isEqualTo(ErrorBucket.SUCCESS);
    }

    @ParameterizedTest
    @CsvSource({
            "es_rejected_execution_exception, 429, TRANSIENT",
            "unavailable_shards_exception, 503, TRANSIENT",
            "node_not_connected_exception, 500, TRANSIENT",
            "node_disconnected_exception, 500, TRANSIENT",
            "circuit_breaking_exception, 429, TRANSIENT",
            "no_shard_available_action_exception, 503, TRANSIENT",
            "process_cluster_event_timeout_exception, 503, TRANSIENT",
            "mapper_parsing_exception, 400, RECORD",
            "document_parsing_exception, 400, RECORD",
            "strict_dynamic_mapping_exception, 400, RECORD",
            "illegal_argument_exception, 400, RECORD",
            "version_conflict_engine_exception, 409, RECORD",
            "document_missing_exception, 404, RECORD",
            "routing_missing_exception, 400, RECORD",
            "cluster_block_exception, 403, RESOURCE_FATAL",
            "index_closed_exception, 400, RESOURCE_FATAL",
            "index_not_found_exception, 404, RESOURCE_FATAL"
    })
    void shouldClassifyKnownErrorTypesRegardlessOfStatus(String type, int status, ErrorBucket expected) {
        assertThat(classifier.classify(item(status, type))).isEqualTo(expected);
        // The type governs: an unexpected status on a known type does not change the bucket.
        assertThat(classifier.classify(item(500, type))).isEqualTo(expected);
    }

    @Test
    void shouldScopeSecurityExceptionByStatus() {
        // A bulk item always names its index, so a 403 here is resource-scoped; a 401 never is.
        assertThat(classifier.classify(item(403, "security_exception"))).isEqualTo(ErrorBucket.RESOURCE_FATAL);
        assertThat(classifier.classify(item(401, "security_exception"))).isEqualTo(ErrorBucket.TASK_FATAL);
    }

    @ParameterizedTest
    @CsvSource({
            "429, TRANSIENT",
            "503, TRANSIENT",
            "400, RECORD",
            "409, RECORD",
            "404, TASK_FATAL",
            "500, TASK_FATAL",
            "502, TASK_FATAL"
    })
    void shouldFallBackToStatusForUnknownErrorTypes(int status, ErrorBucket expected) {
        // DDD-61 9.1: the default for an unmatched outcome is task-fatal, never transient.
        assertThat(classifier.classify(item(status, "some_new_exception"))).isEqualTo(expected);
        assertThat(classifier.classify(item(status, null, true))).isEqualTo(expected);
    }

    @Test
    void shouldHonorOverridesBeforeTheBuiltInTable() {
        final BulkResponseClassifier overridden = new BulkResponseClassifier(Map.of(
                "circuit_breaking_exception", ErrorBucket.RECORD,
                "illegal_argument_exception", ErrorBucket.TRANSIENT,
                "brand_new_exception", ErrorBucket.RESOURCE_FATAL));
        assertThat(overridden.classify(item(429, "circuit_breaking_exception"))).isEqualTo(ErrorBucket.RECORD);
        assertThat(overridden.classify(item(400, "illegal_argument_exception"))).isEqualTo(ErrorBucket.TRANSIENT);
        assertThat(overridden.classify(item(500, "brand_new_exception"))).isEqualTo(ErrorBucket.RESOURCE_FATAL);
        // Untouched entries keep their built-in classification.
        assertThat(overridden.classify(item(400, "mapper_parsing_exception"))).isEqualTo(ErrorBucket.RECORD);
    }

    @Test
    void shouldClassifyIoExceptionAnywhereInCauseChainAsTransient() {
        assertThat(classifier.classify(new IOException("connection reset"))).isEqualTo(ErrorBucket.TRANSIENT);
        assertThat(classifier.classify(new SocketTimeoutException("read timed out"))).isEqualTo(ErrorBucket.TRANSIENT);
        assertThat(classifier.classify(new RuntimeException("wrapped", new IllegalStateException(new IOException("reset")))))
                .isEqualTo(ErrorBucket.TRANSIENT);
    }

    @Test
    void shouldClassifyOtherTransportFailuresAsTaskFatal() {
        assertThat(classifier.classify(new NullPointerException())).isEqualTo(ErrorBucket.TASK_FATAL);
        assertThat(classifier.classify(new IllegalStateException("bad", new RuntimeException()))).isEqualTo(ErrorBucket.TASK_FATAL);
    }

    private static BulkResponseItem item(int status, String errorType) {
        return item(status, errorType, errorType != null);
    }

    private static BulkResponseItem item(int status, String errorType, boolean withError) {
        return BulkResponseItem.of(b -> {
            b.index("customers").status(status).operationType(OperationType.Index);
            if (withError) {
                b.error(e -> {
                    if (errorType != null) {
                        e.type(errorType);
                    }
                    return e.reason("test failure");
                });
            }
            return b;
        });
    }
}
