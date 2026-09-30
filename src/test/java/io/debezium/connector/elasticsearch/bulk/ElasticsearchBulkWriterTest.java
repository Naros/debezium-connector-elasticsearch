/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.bulk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.bulk.ElasticsearchBulkWriter.BulkItem;
import io.debezium.connector.elasticsearch.client.ClusterVersionHandshake;
import io.debezium.connector.elasticsearch.metrics.ElasticsearchSinkConnectorMetrics;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.connector.elasticsearch.util.DebeziumSinkRecordFactory;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.BulkAction;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.ItemResult;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.RecordedRequest;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.StubResponse;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.util.Clock;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

/**
 * The bulk writer's retry, throttle, classification, and stall behavior (DDD-61 7.2, 8, 9.1)
 * against a scripted endpoint and a fake clock, so the outcomes a live cluster cannot produce
 * on demand are asserted deterministically.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchBulkWriterTest {

    private static final String TOPIC = "inventory.customers";

    private final DebeziumSinkRecordFactory factory = new DebeziumSinkRecordFactory();
    private final List<DebeziumSinkRecord> reported = new ArrayList<>();
    private final List<String> reportedMessages = new ArrayList<>();
    private final List<Long> sleeps = new ArrayList<>();
    private final long[] now = { 1_000_000L };
    private final Clock clock = () -> now[0];

    private StubElasticsearch stub;
    private ElasticsearchSinkConnectorMetrics metrics;
    private ClusterVersionHandshake handshake;

    @BeforeEach
    void beforeEach() throws IOException {
        stub = new StubElasticsearch();
        metrics = new ElasticsearchSinkConnectorMetrics("test", "0");
        handshake = new ClusterVersionHandshake(stub.restClient(), 0, 0, 0, sleeps::add);
        handshake.ensureProbed();
        stub.clearRequests();
    }

    @AfterEach
    void afterEach() throws IOException {
        stub.close();
    }

    @Test
    void shouldIssueOneRequestForTheBatchAndCountOutcomes() {
        writer(Map.of()).write(List.of(index("a", "1"), index("a", "2"), delete("a", "3")));

        final List<RecordedRequest> bulks = stub.requests("POST", "/_bulk");
        assertThat(bulks).hasSize(1);
        assertThat(bulks.get(0).bulkActions()).extracting(BulkAction::operation, BulkAction::id)
                .containsExactly(tuple("update", "1"), tuple("update", "2"), tuple("delete", "3"));
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(2);
        assertThat(metrics.getTotalNumberOfDeletes()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfBulkRequests()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfRetries()).isZero();
        assertThat(reported).isEmpty();
        assertThat(sleeps).isEmpty();
    }

    @Test
    void shouldTreatDeleteOfAbsentDocumentAsSuccess() {
        stub.enqueueBulk(a -> new ItemResult(404, null, null));

        writer(Map.of()).write(List.of(delete("a", "gone")));

        assertThat(metrics.getTotalNumberOfDeletes()).isEqualTo(1);
        assertThat(reported).isEmpty();
    }

    @Test
    void shouldRetryOnlyTheRejectedItemWithoutSpendingTheRetryBudget() {
        // DDD-61 7.1 hazard 2 and 8: a 429 on one item retries that item alone, throttles, and
        // is never counted against 'max.retries'.
        stub.enqueueBulk(a -> "2".equals(a.id()) ? ItemResult.error(429, "es_rejected_execution_exception") : ItemResult.ok());

        writer(Map.of(SinkConnectorConfig.BATCH_SIZE, "8", ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "50"))
                .write(List.of(index("a", "1"), index("a", "2"), index("a", "3")));

        final List<RecordedRequest> bulks = stub.requests("POST", "/_bulk");
        assertThat(bulks).hasSize(2);
        assertThat(bulks.get(1).bulkActions()).extracting(BulkAction::id).containsExactly("2");
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(3);
        assertThat(metrics.getTotalNumberOfRetries()).isZero();
        assertThat(metrics.getTotalNumberOfThrottleEvents()).isEqualTo(1);
        // Backpressure is expressed through the halved batch (DDD-61 8), not a pause: a round
        // that made progress retries its rejected remainder immediately.
        assertThat(sleeps).isEmpty();
        assertThat(reported).isEmpty();
    }

    @Test
    void shouldHalveTheBatchAfterRejectionAndRecoverAfterCleanResponses() {
        final AdaptiveThrottle throttle = new AdaptiveThrottle(8, null, null);
        stub.enqueueBulk(a -> ItemResult.error(429, "es_rejected_execution_exception"));

        writer(Map.of(SinkConnectorConfig.BATCH_SIZE, "8"), throttle).write(List.of(index("a", "1")));
        assertThat(throttle.effectiveBatchSize()).isEqualTo(4);

        // Three clean responses recover one tenth of the configured size, with a floor of one.
        final ElasticsearchBulkWriter writer = writer(Map.of(SinkConnectorConfig.BATCH_SIZE, "8"), throttle);
        writer.write(List.of(index("a", "2")));
        writer.write(List.of(index("a", "3")));
        assertThat(throttle.effectiveBatchSize()).isEqualTo(5);
    }

    @Test
    void shouldExhaustTheRetryBudgetOnPersistentNonThrottleTransientFailure() {
        stub.withDefault("POST", "/_bulk"::equals,
                r -> StubElasticsearch.bulkResponse(r.bulkActions(), a -> ItemResult.error(503, "unavailable_shards_exception")));

        assertThatThrownBy(() -> writer(Map.of(ElasticsearchSinkConnectorConfig.MAX_RETRIES, "2",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "100",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "150")).write(List.of(index("a", "1"))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'max.retries'")
                .hasMessageContaining("unavailable_shards_exception");

        assertThat(stub.requests("POST", "/_bulk")).hasSize(3);
        assertThat(metrics.getTotalNumberOfRetries()).isEqualTo(3);
        assertThat(sleeps).as("exponential backoff capped at retry.backoff.max.ms").containsExactly(100L, 150L);
    }

    @Test
    void shouldRouteRecordLevelFailuresToTheReporterAndKeepGoing() {
        stub.enqueueBulk(a -> "1".equals(a.id()) ? ItemResult.error(400, "mapper_parsing_exception") : ItemResult.ok());
        final BulkItem bad = index("a", "1");

        writer(Map.of()).write(List.of(bad, index("a", "2")));

        assertThat(stub.requests("POST", "/_bulk")).hasSize(1);
        assertThat(reported).containsExactly(bad.record());
        assertThat(reportedMessages.get(0)).contains("mapper_parsing_exception").contains("'a'").contains("status 400");
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfErrantRecords()).isEqualTo(1);
    }

    @Test
    void shouldFailTheTaskOnAnUnclassifiedItemOutcome() {
        stub.enqueueBulk(a -> ItemResult.error(500, "some_new_exception"));

        assertThatThrownBy(() -> writer(Map.of()).write(List.of(index("a", "1"))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("failed fatally")
                .hasMessageContaining("some_new_exception");
        assertThat(stub.requests("POST", "/_bulk")).hasSize(1);
        assertThat(reported).isEmpty();
    }

    @Test
    void shouldHonorClassificationOverrides() {
        // A site that knows 'some_new_exception' is per-record can say so without a release.
        stub.enqueueBulk(a -> ItemResult.error(500, "some_new_exception"));

        writer(Map.of(ElasticsearchSinkConnectorConfig.ERROR_CLASSIFICATION_OVERRIDES, "some_new_exception:record"))
                .write(List.of(index("a", "1")));

        assertThat(reported).hasSize(1);
    }

    @Test
    void shouldBlockOnlyTheFailingResourceAndDrainItToTheReporterAfterTheStallTimeout() {
        // DDD-61 9.1: a closed index stops only itself; once 'progress.stall.timeout.ms' elapses
        // its records reach the error reporter rather than being dropped, and the task survives.
        stub.withDefault("POST", "/_bulk"::equals, r -> StubElasticsearch.bulkResponse(r.bulkActions(),
                a -> "closed".equals(a.index()) ? ItemResult.error(400, "index_closed_exception") : ItemResult.ok()));
        final BulkItem blocked = index("closed", "1");

        writer(Map.of(ElasticsearchSinkConnectorConfig.PROGRESS_STALL_TIMEOUT_MS, "1000",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "100",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "400",
                ElasticsearchSinkConnectorConfig.MAX_RETRIES, "0"))
                .write(List.of(blocked, index("open", "2")));

        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(1);
        assertThat(reported).containsExactly(blocked.record());
        assertThat(reportedMessages.get(0)).contains("remained blocked past 'progress.stall.timeout.ms'").contains("index_closed_exception");
        assertThat(metrics.getTotalNumberOfRetries()).as("a blocked resource does not spend the retry budget").isZero();
        assertThat(stub.requests("POST", "/_bulk").stream().skip(1))
                .as("retries carry only the blocked resource")
                .allSatisfy(r -> assertThat(r.bulkActions()).extracting(BulkAction::index).containsOnly("closed"));
    }

    @Test
    void shouldFailTheTaskWhenEveryPendingResourceIsBlockedPastTheStallTimeout() {
        stub.withDefault("POST", "/_bulk"::equals,
                r -> StubElasticsearch.bulkResponse(r.bulkActions(), a -> ItemResult.error(403, "cluster_block_exception")));

        assertThatThrownBy(() -> writer(Map.of(ElasticsearchSinkConnectorConfig.PROGRESS_STALL_TIMEOUT_MS, "500",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "200",
                ElasticsearchSinkConnectorConfig.MAX_RETRIES, "0")).write(List.of(index("a", "1"), index("b", "2"))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("every resource in the batch")
                .hasMessageContaining("cluster_block_exception");
        assertThat(reported).isEmpty();
    }

    @Test
    void shouldFailTheTaskWhenNothingProgressesPastTheStallTimeout() {
        // Transient failures inside the retry budget still cannot outlast the stall timeout.
        stub.withDefault("POST", "/_bulk"::equals,
                r -> StubElasticsearch.bulkResponse(r.bulkActions(), a -> ItemResult.error(429, "es_rejected_execution_exception")));

        assertThatThrownBy(() -> writer(Map.of(ElasticsearchSinkConnectorConfig.PROGRESS_STALL_TIMEOUT_MS, "1000",
                ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "300",
                ElasticsearchSinkConnectorConfig.MAX_RETRIES, "1000")).write(List.of(index("a", "1"))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("No progress for longer than 'progress.stall.timeout.ms'");
        assertThat(metrics.getTotalNumberOfRetries()).as("throttle rounds never count as retries").isZero();
    }

    @Test
    void shouldRetryTheWholeChunkAfterATransportFailure() {
        stub.enqueue("POST", "/_bulk", StubResponse.dropConnection());

        writer(Map.of(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "10")).write(List.of(index("a", "1"), index("a", "2")));

        final List<RecordedRequest> bulks = stub.requests("POST", "/_bulk");
        assertThat(bulks).hasSize(2);
        assertThat(bulks.get(1).bulkActions()).extracting(BulkAction::id).containsExactly("1", "2");
        assertThat(metrics.getTotalNumberOfRetries()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(2);
    }

    @Test
    void shouldReprobeTheClusterVersionAfterAMediaTypeRejection() {
        // DDD-61 10.1.1: a compatibility-shaped error invalidates the cached version and forces
        // a re-probe before the retry, covering a cluster upgraded under a running task.
        stub.enqueue("POST", "/_bulk", StubResponse.error(406, "media_type_header_exception", "Invalid media-type value"));
        stub.withVersion("9.5.1");

        writer(Map.of(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "10")).write(List.of(index("a", "1")));

        assertThat(stub.requests("GET", "/")).hasSize(1);
        assertThat(handshake.ensureProbed().number()).isEqualTo("9.5.1");
        assertThat(stub.requests("POST", "/_bulk")).hasSize(2);
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(1);
    }

    @Test
    void shouldFailTheTaskWhenTheResponseAnswersFewerItemsThanWereSent() {
        // A short response must not pass the unanswered items off as applied.
        stub.enqueue("POST", "/_bulk", StubResponse.ok("{\"took\":1,\"errors\":false,\"items\":[]}"));

        assertThatThrownBy(() -> writer(Map.of()).write(List.of(index("a", "1"), index("a", "2"))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("0 item results for 2 operations");
        assertThat(metrics.getTotalNumberOfWrites()).isZero();
    }

    @Test
    void shouldRetryAMalformedResponseAsATransportFailure() {
        // The client surfaces a body it cannot deserialize as an IOException, so a garbled response
        // is a transport hiccup that spends one retry rather than a task failure.
        stub.enqueue("POST", "/_bulk", StubResponse.ok("{\"took\":1,\"errors\":false,\"items\":[{\"update\":{\"_index\":\"a\",\"status\":\"not-a-number\"}}]}"));

        writer(Map.of(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "10")).write(List.of(index("a", "1")));

        assertThat(stub.requests("POST", "/_bulk")).hasSize(2);
        assertThat(metrics.getTotalNumberOfRetries()).isEqualTo(1);
        assertThat(metrics.getTotalNumberOfWrites()).isEqualTo(1);
    }

    @Test
    void shouldChunkByEffectiveBatchSizeInOrder() {
        writer(Map.of(SinkConnectorConfig.BATCH_SIZE, "2"))
                .write(List.of(index("a", "1"), index("a", "2"), index("a", "3"), index("a", "4"), index("a", "5")));

        final List<RecordedRequest> bulks = stub.requests("POST", "/_bulk");
        assertThat(bulks).hasSize(3);
        assertThat(bulks.get(0).bulkActions()).extracting(BulkAction::id).containsExactly("1", "2");
        assertThat(bulks.get(1).bulkActions()).extracting(BulkAction::id).containsExactly("3", "4");
        assertThat(bulks.get(2).bulkActions()).extracting(BulkAction::id).containsExactly("5");
        assertThat(metrics.getTotalNumberOfBulkRequests()).isEqualTo(3);
    }

    @Test
    void shouldChunkByBytesWithoutSplittingBelowOneItem() {
        writer(Map.of(ElasticsearchSinkConnectorConfig.BULK_SIZE_BYTES, "1000"))
                .write(List.of(index("a", "1", 600), index("a", "2", 300), index("a", "3", 300), index("a", "4", 2000)));

        final List<RecordedRequest> bulks = stub.requests("POST", "/_bulk");
        assertThat(bulks).hasSize(3);
        assertThat(bulks.get(0).bulkActions()).extracting(BulkAction::id).containsExactly("1", "2");
        assertThat(bulks.get(1).bulkActions()).extracting(BulkAction::id).containsExactly("3");
        assertThat(bulks.get(2).bulkActions()).extracting(BulkAction::id).containsExactly("4");
    }

    private ElasticsearchBulkWriter writer(Map<String, String> overrides) {
        return writer(overrides, new AdaptiveThrottle(config(overrides).getBatchSize(), null, null));
    }

    private ElasticsearchBulkWriter writer(Map<String, String> overrides, AdaptiveThrottle throttle) {
        return new ElasticsearchBulkWriter(stub.client(), config(overrides), throttle, (record, error) -> {
            reported.add(record);
            reportedMessages.add(error.getMessage());
        }, metrics, handshake, clock, millis -> {
            sleeps.add(millis);
            now[0] += millis;
        });
    }

    private ElasticsearchSinkConnectorConfig config(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url());
        properties.put(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS, "1");
        properties.put(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "1000");
        properties.putAll(overrides);
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(properties);
        config.validate();
        return config;
    }

    private BulkItem index(String index, String id) {
        return index(index, id, 100);
    }

    private BulkItem index(String index, String id, long bytes) {
        final BulkOperation operation = BulkOperation.of(b -> b.update(u -> u.index(index).id(id)
                .action(a -> a.doc(Map.of("id", id)).docAsUpsert(true))));
        return new BulkItem(batchRecord(index, id), operation, false, bytes);
    }

    private BulkItem delete(String index, String id) {
        return new BulkItem(batchRecord(index, id), BulkOperation.of(b -> b.delete(d -> d.index(index).id(id))), true, 96);
    }

    private BatchRecord batchRecord(String index, String id) {
        final DebeziumSinkRecord record = new ElasticsearchSinkRecord(
                factory.createRecord(TOPIC, id.hashCode(), id, null, 0), null);
        return new BatchRecord(new CollectionId(index), record);
    }

    private static org.assertj.core.groups.Tuple tuple(Object... values) {
        return org.assertj.core.groups.Tuple.tuple(values);
    }
}
