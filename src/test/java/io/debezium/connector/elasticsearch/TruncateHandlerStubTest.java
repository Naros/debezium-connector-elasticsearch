/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.elasticsearch.mapping.MappingManager;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.RecordedRequest;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.StubResponse;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;

/**
 * The truncate handler's cluster-facing paths (DDD-61 5.1) against a scripted endpoint: the
 * delete_by_query request shape, an aborted truncate on concurrent writes, recreation through
 * the mapping manager, and transport failures.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class TruncateHandlerStubTest {

    private static final String TOPIC = "inventory.customers";
    private static final String RESOURCE = "inventory.customers";
    private static final String DELETE_BY_QUERY_PATH = "/" + RESOURCE + "/_delete_by_query";
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("Value").field("id", Schema.INT32_SCHEMA).build();

    private StubElasticsearch stub;

    @BeforeEach
    void beforeEach() throws IOException {
        stub = new StubElasticsearch();
    }

    @AfterEach
    void afterEach() throws IOException {
        stub.close();
    }

    @Test
    void shouldIssueMatchAllDeleteByQueryAgainstTheResolvedResource() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, "inventory.*"));
        stub.enqueue("POST", DELETE_BY_QUERY_PATH, StubResponse.ok(deleteByQueryResponse(7, 0)));

        assertThatCode(() -> handler.truncate(record(), RESOURCE)).doesNotThrowAnyException();

        final List<RecordedRequest> requests = stub.requests();
        // The index is refreshed first so documents the drained bulk request just indexed are
        // visible to the delete_by_query snapshot.
        assertThat(requests).hasSize(2);
        assertThat(requests.get(0).method()).isEqualTo("POST");
        assertThat(requests.get(0).path()).isEqualTo("/" + RESOURCE + "/_refresh");
        assertThat(requests.get(1).method()).isEqualTo("POST");
        assertThat(requests.get(1).path()).isEqualTo(DELETE_BY_QUERY_PATH);
        assertThat(requests.get(1).body()).contains("\"match_all\"");
        // DDD-61 5.1: abort on a concurrent write rather than proceed partially, refresh so the
        // following writes see the empty index, and wait rather than return a task handle.
        assertThat(requests.get(1).queryParameters())
                .containsEntry("conflicts", "abort")
                .containsEntry("refresh", "true")
                .containsEntry("wait_for_completion", "true");
    }

    @Test
    void shouldAbortTruncateOnConcurrentWriteConflictsRatherThanReportPartialSuccess() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        stub.enqueue("POST", DELETE_BY_QUERY_PATH, StubResponse.ok(deleteByQueryResponse(3, 2)));

        // DDD-61 5.1: conflicts=abort makes a concurrent write fail the truncate loudly and stop
        // the task, instead of applying to part of the index and reporting success.
        assertThatThrownBy(() -> handler.truncate(record(), RESOURCE))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(RESOURCE)
                .hasMessageContaining("2 concurrent write conflicts")
                .hasMessageContaining("not fully applied");
    }

    @Test
    void shouldSurfaceATransportFailureDuringDeleteByQueryAsATaskFailure() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        stub.enqueue("POST", DELETE_BY_QUERY_PATH, StubResponse.dropConnection());

        assertThatThrownBy(() -> handler.truncate(record(), RESOURCE))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("delete_by_query")
                .hasMessageContaining(RESOURCE)
                .cause().isInstanceOf(IOException.class);
    }

    @Test
    void shouldSurfaceAClusterErrorDuringDeleteByQueryAsATaskFailure() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        stub.enqueue("POST", DELETE_BY_QUERY_PATH, StubResponse.error(403, "cluster_block_exception", "index read-only"));

        assertThatThrownBy(() -> handler.truncate(record(), RESOURCE))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(RESOURCE)
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void shouldDeleteTheIndexAndLetTheMappingManagerRebuildItUnderRecreate() {
        final ElasticsearchSinkConnectorConfig config = config(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "recreate",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        final MappingManager mappingManager = new MappingManager(stub.client(), config, "test");
        final TruncateHandler handler = new TruncateHandler(stub.client(), config, mappingManager);

        // The resource is ensured once before the truncate, and memoized.
        mappingManager.ensureResource(RESOURCE, VALUE_SCHEMA);
        final int requestsToEnsure = stub.requests().size();
        assertThat(requestsToEnsure).isPositive();
        mappingManager.ensureResource(RESOURCE, VALUE_SCHEMA);
        assertThat(stub.requests()).as("memoized before the truncate").hasSize(requestsToEnsure);
        stub.clearRequests();

        handler.truncate(record(), RESOURCE);

        assertThat(stub.requests()).hasSize(1);
        assertThat(stub.requests().get(0).method()).isEqualTo("DELETE");
        assertThat(stub.requests().get(0).path()).isEqualTo("/" + RESOURCE);
        stub.clearRequests();

        // The truncate invalidated the memo, so the next write re-applies the template and index.
        mappingManager.ensureResource(RESOURCE, VALUE_SCHEMA);
        assertThat(stub.requests()).hasSize(requestsToEnsure);
        assertThat(stub.requests()).extracting(RecordedRequest::method).contains("PUT");
        assertThat(stub.requests()).extracting(RecordedRequest::path).contains("/" + RESOURCE);
    }

    @Test
    void shouldNotFailRecreateWhenTheIndexIsAlreadyAbsent() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "recreate",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        // ignore_unavailable=true: the cluster acknowledges rather than reporting index_not_found.
        stub.enqueue("DELETE", "/" + RESOURCE, StubResponse.ok("{\"acknowledged\":true}"));

        assertThatCode(() -> handler.truncate(record(), RESOURCE)).doesNotThrowAnyException();
        assertThat(stub.requests("DELETE", "/" + RESOURCE)).hasSize(1);
    }

    @Test
    void shouldSurfaceATransportFailureDuringRecreateAsATaskFailure() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "recreate",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, RESOURCE));
        stub.enqueue("DELETE", "/" + RESOURCE, StubResponse.dropConnection());

        assertThatThrownBy(() -> handler.truncate(record(), RESOURCE))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("recreate")
                .hasMessageContaining(RESOURCE)
                .cause().isInstanceOf(IOException.class);
    }

    @Test
    void shouldIssueNoRequestForAResourceOutsideTheAllowList() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, "staging-*"));

        // DDD-61 5.1: outside the list is a record-level error naming the resource and the list.
        assertThatThrownBy(() -> handler.truncate(record(), RESOURCE))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining(RESOURCE)
                .hasMessageContaining("staging-*");
        assertThat(stub.requests()).isEmpty();
    }

    private TruncateHandler handler(Map<String, String> overrides) {
        final ElasticsearchSinkConnectorConfig config = config(overrides);
        return new TruncateHandler(stub.client(), config, new MappingManager(stub.client(), config, "test"));
    }

    private ElasticsearchSinkConnectorConfig config(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url());
        properties.putAll(overrides);
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(properties);
        config.validate();
        return config;
    }

    private static DebeziumSinkRecord record() {
        final Schema sourceSchema = SchemaBuilder.struct().name("Source").field("ts_ms", Schema.INT64_SCHEMA).build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name(TOPIC + ".Envelope")
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final Struct envelope = new Struct(envelopeSchema)
                .put(Envelope.FieldName.SOURCE, new Struct(sourceSchema).put("ts_ms", Instant.now().toEpochMilli()))
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.TRUNCATE.code())
                .put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        return new ElasticsearchSinkRecord(new SinkRecord(TOPIC, 0, null, null, envelopeSchema, envelope, 0), null);
    }

    private static String deleteByQueryResponse(int deleted, int versionConflicts) {
        return "{\"took\":1,\"timed_out\":false,\"total\":" + (deleted + versionConflicts) + ",\"deleted\":" + deleted
                + ",\"batches\":1,\"version_conflicts\":" + versionConflicts + ",\"noops\":0,\"retries\":{\"bulk\":0,\"search\":0},"
                + "\"throttled_millis\":0,\"requests_per_second\":-1,\"throttled_until_millis\":0,\"failures\":[]}";
    }
}
