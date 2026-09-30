/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.connector.elasticsearch.mapping.MappingManager;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.sink.DebeziumSinkRecord;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;

/**
 * Unit tests for the pure parts of {@link TruncateHandler}: the {@code truncate.mode} switch and
 * the {@code truncate.allowed.resources} gate of DDD-61 section 5.1. The destructive calls
 * themselves need a cluster and are covered by integration tests.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class TruncateHandlerTest {

    private static final String TOPIC = "inventory.customers";
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

    private final ElasticsearchClient client = mock(ElasticsearchClient.class);
    private final MappingManager mappingManager = mock(MappingManager.class);

    @Test
    void shouldIgnoreTruncateByDefaultWithoutTouchingTheCluster() {
        final TruncateHandler handler = handler(Map.of());
        assertThatCode(() -> handler.truncate(record(), "inventory.customers")).doesNotThrowAnyException();
        verifyNoInteractions(client, mappingManager);
    }

    @Test
    void shouldFailTheTaskWhenModeIsFail() {
        final TruncateHandler handler = handler(Map.of(ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "fail"));
        assertThatThrownBy(() -> handler.truncate(record(), "inventory.customers"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("inventory.customers")
                .hasMessageContaining(ElasticsearchSinkConnectorConfig.TRUNCATE_MODE);
        verifyNoInteractions(client, mappingManager);
    }

    @ParameterizedTest
    @CsvSource({ "delete_by_query", "recreate" })
    void shouldRouteTruncateOutsideAllowListToErrorHandlerWithoutTouchingTheCluster(String mode) {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, mode,
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, "inventory.orders, staging-*"));
        // DDD-61 5.1: a truncate outside the list is a record-level error, never a silent no-op.
        assertThatThrownBy(() -> handler.truncate(record(), "inventory.customers"))
                .isInstanceOf(RecordProcessingException.class)
                .hasMessageContaining("inventory.customers")
                .hasMessageContaining(ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES)
                .hasMessageContaining("inventory.orders");
        verifyNoInteractions(client, mappingManager);
    }

    @ParameterizedTest
    @CsvSource({
            "inventory.customers, inventory.customers, true",
            "inventory.customers, inventory.customers2, false",
            "inventory.*, inventory.customers, true",
            "inventory.*, inventory., true",
            "inventory.*, staging.customers, false",
            "*-customers, inventory-customers, true",
            "inventory.customers-?, inventory.customers-1, true",
            "inventory.customers-?, inventory.customers-10, false",
            "inventory.customers, inventoryXcustomers, false",
            "'inventory.orders, inventory.customers', inventory.customers, true"
    })
    @SuppressWarnings("unchecked")
    void shouldMatchAllowListAsGlobOnResolvedResource(String allowed, String resource, boolean matches) throws IOException {
        // A pass through the gate reaches the mocked client, which fails the destructive call;
        // that wrapped failure is what proves the gate was passed.
        // Both overloads are stubbed so the outcome does not depend on the mock maker in use.
        doThrow(new IOException("no cluster")).when(client).deleteByQuery(any(DeleteByQueryRequest.class));
        doThrow(new IOException("no cluster")).when(client).deleteByQuery(any(Function.class));
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, allowed));
        if (matches) {
            assertThatThrownBy(() -> handler.truncate(record(), resource))
                    .isInstanceOf(ConnectException.class)
                    .hasMessageContaining("delete_by_query");
        }
        else {
            assertThatThrownBy(() -> handler.truncate(record(), resource))
                    .isInstanceOf(RecordProcessingException.class);
            verifyNoInteractions(client);
        }
    }

    @Test
    void shouldMatchOnResolvedResourceNotTopic() {
        final TruncateHandler handler = handler(Map.of(
                ElasticsearchSinkConnectorConfig.TRUNCATE_MODE, "delete_by_query",
                ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, TOPIC));
        // The topic is on the list, the resolved resource is not: DDD-61 5.1 matches the resource.
        assertThatThrownBy(() -> handler.truncate(record(), "customers-2024"))
                .isInstanceOf(RecordProcessingException.class);
        verifyNoInteractions(client);
    }

    private TruncateHandler handler(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "http://localhost:9200");
        properties.putAll(overrides);
        return new TruncateHandler(client, new ElasticsearchSinkConnectorConfig(properties), mappingManager);
    }

    private static DebeziumSinkRecord record() {
        final Struct key = new Struct(KEY_SCHEMA).put("id", 1);
        final Struct value = new Struct(VALUE_SCHEMA).put("id", 1);
        return new ElasticsearchSinkRecord(new SinkRecord(TOPIC, 0, KEY_SCHEMA, key, VALUE_SCHEMA, value, 7), ".*CloudEvents\\.Envelope$");
    }
}
