/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkTaskTestContext.ReportedRecord;
import io.debezium.connector.elasticsearch.junit.ElasticsearchTestCluster;
import io.debezium.connector.elasticsearch.junit.jupiter.ElasticsearchExtension;
import io.debezium.connector.elasticsearch.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.elasticsearch.util.SinkRecordFactory;

import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch.core.GetResponse;

/**
 * Smoke test: drives {@link ElasticsearchSinkConnectorTask} against a live cluster through the
 * Kafka Connect lifecycle, exercising client construction, the version handshake, template and
 * index creation, bulk writes and deletes, offset commit, and the errant record path.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@ExtendWith(ElasticsearchExtension.class)
public class ElasticsearchSinkConnectorTaskIT {

    private static final String TOPIC = "inventory.customers";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private ElasticsearchTestCluster cluster;
    private ElasticsearchSinkConnectorTask task;
    private ElasticsearchSinkTaskTestContext context;

    @BeforeEach
    void beforeEach(ElasticsearchTestCluster cluster) throws IOException {
        this.cluster = cluster;
        cluster.reset(List.of(TOPIC));
    }

    @AfterEach
    void afterEach() {
        if (task != null) {
            task.stop();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldApplyCreateUpdateAndDeleteAcrossCommits(SinkRecordFactory factory) throws IOException {
        startTask(Map.of());

        task.put(List.of(
                factory.createRecord(TOPIC, 1, "alice", "alice@example.com", 0),
                factory.createRecord(TOPIC, 2, "bob", "bob@example.com", 1)));
        // The task tracks its own confirmed offsets and must not fall back to Connect's.
        Map<TopicPartition, OffsetAndMetadata> committed = task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));
        assertThat(committed).containsEntry(PARTITION, new OffsetAndMetadata(2));

        assertThat(document("1")).containsEntry("id", 1).containsEntry("name", "alice").containsEntry("email", "alice@example.com");
        assertThat(document("2")).containsEntry("id", 2).containsEntry("name", "bob");

        task.put(List.of(
                factory.updateRecord(TOPIC, 1, "alice", "alice@debezium.io", 2),
                factory.deleteRecord(TOPIC, 2, 3)));
        committed = task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));
        assertThat(committed).containsEntry(PARTITION, new OffsetAndMetadata(4));

        assertThat(document("1")).containsEntry("id", 1).containsEntry("name", "alice").containsEntry("email", "alice@debezium.io");
        assertThat(exists("2")).isFalse();
        assertThat(context.reportedRecords()).isEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldReduceInterleavedChangesForOneKeyWithinOneBatch(SinkRecordFactory factory) throws IOException {
        startTask(Map.of());

        // c,u,d for key 1 must end as a delete; d,c for key 2 must end as a write.
        task.put(List.of(
                factory.createRecord(TOPIC, 1, "first", null, 0),
                factory.updateRecord(TOPIC, 1, "second", null, 1),
                factory.deleteRecord(TOPIC, 1, 2),
                factory.createRecord(TOPIC, 2, "before", null, 3),
                factory.deleteRecord(TOPIC, 2, 4),
                factory.createRecord(TOPIC, 2, "after", null, 5)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(6)));

        assertThat(exists("1")).isFalse();
        assertThat(document("2")).containsEntry("name", "after");
        assertThat(context.reportedRecords()).isEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldComposeDocumentIdInConfiguredFieldOrder(SinkRecordFactory factory) throws IOException {
        // Key schema order is (tenant, id); the configured order, with stray whitespace, is the reverse.
        startTask(Map.of("primary.key.fields", "id, tenant"));

        task.put(List.of(factory.createRecordWithCompositeKey(TOPIC, "acme", 7, "alice", 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        assertThat(document("7:acme")).containsEntry("name", "alice");
        assertThat(exists("acme:7")).isFalse();
        assertThat(context.reportedRecords()).isEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldRouteStrictMappingRejectionToErrantRecordReporter(SinkRecordFactory factory) throws IOException {
        // A user-owned strict index under mapping.mode=none: the connector generates nothing, so a
        // record carrying a field the mapping does not know is rejected per item rather than per
        // request, and routes to the reporter while its neighbors land.
        cluster.client().indices().create(c -> c.index(TOPIC).mappings(m -> m
                .dynamic(DynamicMapping.Strict)
                .properties("id", p -> p.integer(i -> i))
                .properties("name", p -> p.keyword(k -> k))
                .properties("email", p -> p.keyword(k -> k))
                .properties("__op", p -> p.keyword(k -> k))
                .properties("__deleted", p -> p.keyword(k -> k))));
        startTask(Map.of("errors.tolerance", "all", ElasticsearchSinkConnectorConfig.MAPPING_MODE, "none"));

        final SinkRecord accepted = factory.createRecord(TOPIC, 1, "alice", null, 0);
        final SinkRecord rejected = factory.createRecordWithExtraField(TOPIC, 2, "bob", "unmapped", "value", 1);
        task.put(List.of(accepted));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(1)));
        task.put(List.of(rejected, factory.createRecord(TOPIC, 3, "carol", null, 2)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(3)));

        assertThat(document("1")).containsEntry("name", "alice");
        assertThat(document("3")).containsEntry("name", "carol");
        assertThat(exists("2")).isFalse();

        assertThat(context.reportedRecords()).hasSize(1);
        final ReportedRecord reported = context.reportedRecords().get(0);
        assertThat(reported.record()).isSameAs(rejected);
        assertThat(reported.error()).hasMessageContaining("strict_dynamic_mapping_exception");
    }

    private void startTask(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>(cluster.connectionProperties());
        properties.put("name", "smoke");
        properties.putAll(overrides);

        context = new ElasticsearchSinkTaskTestContext(properties);
        task = new ElasticsearchSinkConnectorTask();
        task.initialize(context);
        task.start(properties);
        task.open(List.of(PARTITION));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> document(String id) throws IOException {
        final GetResponse<Map> response = cluster.client().get(g -> g.index(TOPIC).id(id), Map.class);
        assertThat(response.found()).as("document %s in %s", id, TOPIC).isTrue();
        return response.source();
    }

    private boolean exists(String id) throws IOException {
        return cluster.client().exists(e -> e.index(TOPIC).id(id)).value();
    }
}
