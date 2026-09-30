/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.elasticsearch.junit.ElasticsearchTestCluster;
import io.debezium.connector.elasticsearch.junit.jupiter.ElasticsearchExtension;
import io.debezium.connector.elasticsearch.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.elasticsearch.util.DebeziumSinkRecordFactory;
import io.debezium.connector.elasticsearch.util.SinkRecordFactory;

/**
 * Truncate safety against a live cluster (DDD-61 5.1): the destructive modes empty or recreate
 * only a resource on the allow list, sequence around the writes in the same batch, and every
 * other shape is either rejected at startup, routed to the error handler, or stops the task.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@ExtendWith(ElasticsearchExtension.class)
public class ElasticsearchSinkTruncateIT {

    private static final String TOPIC = "inventory.customers";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final SinkRecordFactory envelopes = new DebeziumSinkRecordFactory();

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
    void shouldDeleteByQueryOnlyWhatPrecededTheTruncateInTheBatch(SinkRecordFactory factory) throws IOException {
        startTask(Map.of("truncate.mode", "delete_by_query", "truncate.allowed.resources", "inventory.*"));
        task.put(List.of(factory.createRecord(TOPIC, 1, "before", null, 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));
        assertThat(exists("1")).isTrue();

        // 5.1 sequencing: writes ahead of the truncate are drained first, the truncate runs to
        // completion, and only then is the next bulk request built.
        task.put(List.of(
                factory.createRecord(TOPIC, 2, "also before", null, 1),
                factory.truncateRecord(TOPIC, 2),
                factory.createRecord(TOPIC, 3, "after", null, 3)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        assertThat(exists("1")).isFalse();
        assertThat(exists("2")).isFalse();
        assertThat(document("3")).containsEntry("name", "after");
        assertThat(indexHasMapping()).as("delete_by_query keeps the index and its mapping").isTrue();
        assertThat(context.reportedRecords()).isEmpty();
    }

    @Test
    void shouldRecreateTheIndexFromTheGeneratedTemplate() throws IOException {
        startTask(Map.of("truncate.mode", "recreate", "truncate.allowed.resources", TOPIC));
        task.put(List.of(envelopes.createRecord(TOPIC, 1, "before", null, 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));
        final String uuidBefore = indexUuid();

        task.put(List.of(envelopes.truncateRecord(TOPIC, 1), envelopes.createRecord(TOPIC, 2, "after", null, 2)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        assertThat(exists("1")).isFalse();
        assertThat(document("2")).containsEntry("name", "after");
        assertThat(indexUuid()).as("a new index, not the old one emptied").isNotEqualTo(uuidBefore);
        assertThat(indexHasMapping()).as("rebuilt from the generated template").isTrue();
        assertThat(context.reportedRecords()).isEmpty();
    }

    @Test
    void shouldRouteATruncateOutsideTheAllowListToTheReporterAndLeaveTheIndexIntact() throws IOException {
        startTask(Map.of("truncate.mode", "delete_by_query", "truncate.allowed.resources", "some.other.index",
                "errors.tolerance", "all"));
        task.put(List.of(envelopes.createRecord(TOPIC, 1, "kept", null, 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        final SinkRecord truncate = envelopes.truncateRecord(TOPIC, 1);
        task.put(List.of(truncate, envelopes.createRecord(TOPIC, 2, "still writing", null, 2)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        assertThat(document("1")).containsEntry("name", "kept");
        assertThat(document("2")).containsEntry("name", "still writing");
        assertThat(context.reportedRecords()).hasSize(1);
        assertThat(context.reportedRecords().get(0).record()).isSameAs(truncate);
        assertThat(context.reportedRecords().get(0).error())
                .hasMessageContaining("'truncate.allowed.resources'")
                .hasMessageContaining(TOPIC);
    }

    @Test
    void shouldStopTheTaskUnderFailMode() throws IOException {
        // DDD-61 5.1: 'fail' is for deployments that handle a truncate manually; the task stops. The
        // task surfaces a put failure on the following put.
        startTask(Map.of("truncate.mode", "fail"));
        task.put(List.of(envelopes.createRecord(TOPIC, 1, "kept", null, 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        task.put(List.of(envelopes.truncateRecord(TOPIC, 1)));
        assertThatThrownBy(() -> task.put(List.of(envelopes.createRecord(TOPIC, 2, "never", null, 2))))
                .isInstanceOf(ConnectException.class)
                .cause().hasMessageContaining("'truncate.mode' is 'fail'");

        assertThat(document("1")).containsEntry("name", "kept");
        assertThat(exists("2")).isFalse();
    }

    @Test
    void shouldDiscardTruncatesByDefault() throws IOException {
        startTask(Map.of());
        task.put(List.of(envelopes.createRecord(TOPIC, 1, "kept", null, 0)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        task.put(List.of(envelopes.truncateRecord(TOPIC, 1), envelopes.createRecord(TOPIC, 2, "more", null, 2)));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));

        assertThat(document("1")).containsEntry("name", "kept");
        assertThat(document("2")).containsEntry("name", "more");
        assertThat(context.reportedRecords()).isEmpty();
    }

    @Test
    void shouldRefuseADestructiveModeWithoutAnAllowListAtStartup() {
        assertThatThrownBy(() -> startTask(Map.of("truncate.mode", "delete_by_query")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'truncate.mode")
                .hasMessageContaining("'truncate.allowed.resources'");
    }

    @Test
    void shouldRefuseABareWildcardWithoutAcknowledgementAtStartup() {
        assertThatThrownBy(() -> startTask(Map.of("truncate.mode", "recreate", "truncate.allowed.resources", "*")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'truncate.allowed.resources'")
                .hasMessageContaining("'truncate.allow.wildcard");
    }

    private void startTask(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>(cluster.connectionProperties());
        properties.put("name", "truncate");
        properties.putAll(overrides);

        context = new ElasticsearchSinkTaskTestContext(properties);
        task = new ElasticsearchSinkConnectorTask();
        task.initialize(context);
        task.start(properties);
        task.open(List.of(PARTITION));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> document(String id) throws IOException {
        final var response = cluster.client().get(g -> g.index(TOPIC).id(id), Map.class);
        assertThat(response.found()).as("document %s in %s", id, TOPIC).isTrue();
        return response.source();
    }

    private boolean exists(String id) throws IOException {
        return cluster.client().exists(e -> e.index(TOPIC).id(id)).value();
    }

    private String indexUuid() throws IOException {
        return cluster.client().indices().get(g -> g.index(TOPIC)).get(TOPIC).settings().index().uuid();
    }

    private boolean indexHasMapping() throws IOException {
        final var mappings = cluster.client().indices().get(g -> g.index(TOPIC)).get(TOPIC).mappings();
        return mappings != null && mappings.properties().containsKey("name");
    }
}
