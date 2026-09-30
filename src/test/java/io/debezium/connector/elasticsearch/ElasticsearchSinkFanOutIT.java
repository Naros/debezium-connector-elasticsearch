/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Year;
import java.time.ZoneOffset;
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
 * Fan-out across many indices from one task (DDD-61 7.2, 9.1): one batch writes many
 * resources, the same document id in different resources survives batch reduction, a single
 * closed index blocks only itself and drains to the error handler after the stall timeout, and
 * a batch with every resource blocked fails the task.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@ExtendWith(ElasticsearchExtension.class)
public class ElasticsearchSinkFanOutIT {

    private static final String CUSTOMERS = "inventory.customers";
    private static final String ORDERS = "inventory.orders";
    private static final String PRODUCTS = "inventory.products";
    private static final String SHARED = "inventory.shared";
    private static final String DATED = CUSTOMERS + "-" + Year.now(ZoneOffset.UTC);

    private final SinkRecordFactory envelopes = new DebeziumSinkRecordFactory();

    private ElasticsearchTestCluster cluster;
    private ElasticsearchSinkConnectorTask task;
    private ElasticsearchSinkTaskTestContext context;

    @BeforeEach
    void beforeEach(ElasticsearchTestCluster cluster) throws IOException {
        this.cluster = cluster;
        cluster.reset(List.of(CUSTOMERS, ORDERS, PRODUCTS, SHARED, DATED));
    }

    @AfterEach
    void afterEach() {
        if (task != null) {
            task.stop();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldWriteManyIndicesFromOneBatch(SinkRecordFactory factory) throws IOException {
        startTask(Map.of());

        task.put(List.of(
                factory.createRecord(CUSTOMERS, 1, "alice", null, 0),
                factory.createRecord(ORDERS, 10, "order-10", null, 0),
                factory.createRecord(PRODUCTS, 100, "widget", null, 0),
                factory.createRecord(ORDERS, 11, "order-11", null, 1)));
        preCommit();

        assertThat(document(CUSTOMERS, "1")).containsEntry("name", "alice");
        assertThat(document(ORDERS, "10")).containsEntry("name", "order-10");
        assertThat(document(ORDERS, "11")).containsEntry("name", "order-11");
        assertThat(document(PRODUCTS, "100")).containsEntry("name", "widget");
        assertThat(context.reportedRecords()).isEmpty();
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    void shouldKeepTheSameIdInDifferentIndicesThroughBatchReduction(SinkRecordFactory factory) throws IOException {
        // DDD-61 7.2: reduction keys on (resource, id), so two topics carrying id=1 must not displace
        // each other.
        startTask(Map.of());

        task.put(List.of(
                factory.createRecord(CUSTOMERS, 1, "customer one", null, 0),
                factory.createRecord(ORDERS, 1, "order one", null, 0)));
        preCommit();

        assertThat(document(CUSTOMERS, "1")).containsEntry("name", "customer one");
        assertThat(document(ORDERS, "1")).containsEntry("name", "order one");
    }

    @Test
    void shouldBlockOnlyAClosedIndexAndDrainItToTheReporterAfterTheStallTimeout() throws IOException {
        startTask(Map.of("errors.tolerance", "all", "progress.stall.timeout.ms", "2000",
                "retry.backoff.ms", "100", "retry.backoff.max.ms", "500"));
        task.put(List.of(envelopes.createRecord(CUSTOMERS, 1, "alice", null, 0), envelopes.createRecord(ORDERS, 1, "order", null, 0)));
        preCommit();
        cluster.client().indices().close(c -> c.index(ORDERS));

        final SinkRecord blocked = envelopes.createRecord(ORDERS, 2, "blocked", null, 1);
        task.put(List.of(envelopes.createRecord(CUSTOMERS, 2, "bob", null, 1), blocked));
        preCommit();

        // DDD-61 9.1: the open index kept writing, the task is alive, and the blocked record reached
        // the error handler rather than being dropped.
        assertThat(document(CUSTOMERS, "2")).containsEntry("name", "bob");
        assertThat(context.reportedRecords()).hasSize(1);
        assertThat(context.reportedRecords().get(0).record()).isSameAs(blocked);
        assertThat(context.reportedRecords().get(0).error())
                .hasMessageContaining("remained blocked past 'progress.stall.timeout.ms'")
                .hasMessageContaining(ORDERS)
                .hasMessageContaining("index_closed_exception");

        task.put(List.of(envelopes.createRecord(CUSTOMERS, 3, "carol", null, 2)));
        preCommit();
        assertThat(document(CUSTOMERS, "3")).containsEntry("name", "carol");
    }

    @Test
    void shouldFailTheTaskWhenEveryIndexInTheBatchIsClosed() throws IOException {
        startTask(Map.of("errors.tolerance", "all", "progress.stall.timeout.ms", "1500",
                "retry.backoff.ms", "100", "retry.backoff.max.ms", "500"));
        task.put(List.of(envelopes.createRecord(CUSTOMERS, 1, "alice", null, 0), envelopes.createRecord(ORDERS, 1, "order", null, 0)));
        preCommit();
        cluster.client().indices().close(c -> c.index(CUSTOMERS));
        cluster.client().indices().close(c -> c.index(ORDERS));

        // The failing put stores the failure; the task surfaces it on the next put.
        task.put(List.of(envelopes.createRecord(CUSTOMERS, 2, "bob", null, 1), envelopes.createRecord(ORDERS, 2, "order", null, 1)));
        assertThatThrownBy(() -> task.put(List.of(envelopes.createRecord(CUSTOMERS, 3, "carol", null, 2))))
                .isInstanceOf(ConnectException.class)
                .cause()
                .hasMessageContaining("progress.stall.timeout.ms")
                .hasMessageContaining("every resource in the batch");
        assertThat(context.reportedRecords()).as("nothing is drained when there is no progress to protect").isEmpty();
    }

    @Test
    void shouldRouteTwoTopicsIntoOneIndexThroughTheTopicToResourceMapping() throws IOException {
        startTask(Map.of("topic.to.resource.mapping", CUSTOMERS + ":" + SHARED + "," + ORDERS + ":" + SHARED));

        task.put(List.of(
                envelopes.createRecord(CUSTOMERS, 1, "customer", null, 0),
                envelopes.createRecord(ORDERS, 2, "order", null, 0)));
        preCommit();

        assertThat(document(SHARED, "1")).containsEntry("name", "customer");
        assertThat(document(SHARED, "2")).containsEntry("name", "order");
        assertThat(cluster.client().indices().exists(e -> e.index(CUSTOMERS)).value()).isFalse();
    }

    @Test
    void shouldResolveADateBasedIndexNameFromTheEventTimestamp() throws IOException {
        startTask(Map.of("collection.name.format", "${topic}-${date:yyyy}", "resource.name.timezone", "UTC"));

        task.put(List.of(envelopes.createRecord(CUSTOMERS, 1, "dated", null, 0)));
        preCommit();

        assertThat(document(DATED, "1")).containsEntry("name", "dated");
        assertThat(cluster.client().indices().exists(e -> e.index(CUSTOMERS)).value()).isFalse();
    }

    private void startTask(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>(cluster.connectionProperties());
        properties.put("name", "fanout");
        properties.putAll(overrides);

        context = new ElasticsearchSinkTaskTestContext(properties);
        task = new ElasticsearchSinkConnectorTask();
        task.initialize(context);
        task.start(properties);
        task.open(List.of(new TopicPartition(CUSTOMERS, 0), new TopicPartition(ORDERS, 0), new TopicPartition(PRODUCTS, 0)));
    }

    private void preCommit() {
        task.preCommit(Map.of(new TopicPartition(CUSTOMERS, 0), new OffsetAndMetadata(0)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> document(String index, String id) throws IOException {
        final var response = cluster.client().get(g -> g.index(index).id(id), Map.class);
        assertThat(response.found()).as("document %s in %s", id, index).isTrue();
        return response.source();
    }
}
