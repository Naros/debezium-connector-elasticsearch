/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.RecordedRequest;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.StubResponse;

/**
 * The template and index creation sequence of {@link MappingManager} (DDD-61 4.2, 6.3) against
 * a scripted endpoint: what is sent, in which order, when it is skipped, and how a conflict is
 * diagnosed.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class MappingManagerTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String RESOURCE = "inventory.customers";
    private static final String TEMPLATE = "debezium-orders-sink-" + RESOURCE;

    private static final Schema SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema EVOLVED_SCHEMA = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

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
    void shouldApplyComponentTemplateThenIndexTemplateThenCreateTheIndexInThatOrder() throws IOException {
        manager(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_COMPOSED_OF, "ilm-hot, my-analyzers")).ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests()).extracting(RecordedRequest::method, RecordedRequest::path).containsExactly(
                tuple("PUT", "/_component_template/" + TEMPLATE),
                tuple("HEAD", "/_index_template/" + TEMPLATE),
                tuple("PUT", "/_index_template/" + TEMPLATE),
                tuple("HEAD", "/" + RESOURCE),
                tuple("PUT", "/" + RESOURCE));

        final JsonNode component = body(stub.requests("PUT", "/_component_template/" + TEMPLATE).get(0));
        assertThat(component.at("/template/mappings/dynamic").asText()).isEqualTo("strict");
        assertThat(component.at("/template/mappings/properties/id/type").asText()).isEqualTo("integer");
        assertThat(component.at("/template/mappings/properties/name/type").asText()).isEqualTo("text");
        assertThat(component.at("/template/settings").isMissingNode()).as("component template carries only mappings").isTrue();

        final JsonNode index = body(stub.requests("PUT", "/_index_template/" + TEMPLATE).get(0));
        assertThat(index.at("/index_patterns")).extracting(JsonNode::asText).containsExactly(RESOURCE);
        assertThat(index.at("/composed_of")).extracting(JsonNode::asText)
                .as("user templates compose ahead of the connector's own")
                .containsExactly("ilm-hot", "my-analyzers", TEMPLATE);
        // DDD-variants entry 5: the seam is priority-based, so the generated template carries no
        // priority and user templates at priority 1 or higher win.
        assertThat(index.has("priority")).isFalse();
        assertThat(index.at("/template").isMissingNode()).as("no settings unless configured").isTrue();
    }

    @Test
    void shouldMemoizePerResourceAndSchemaAndRegenerateOnEvolutionOrInvalidation() {
        final MappingManager manager = manager(Map.of());

        manager.ensureResource(RESOURCE, SCHEMA);
        final int firstPass = stub.requests().size();
        manager.ensureResource(RESOURCE, SCHEMA);
        assertThat(stub.requests()).as("same resource and schema issue nothing").hasSize(firstPass);

        manager.ensureResource(RESOURCE, EVOLVED_SCHEMA);
        assertThat(stub.requests("PUT", "/_component_template/" + TEMPLATE)).as("a new column regenerates the template").hasSize(2);
        assertThat(body(stub.requests("PUT", "/_component_template/" + TEMPLATE).get(1)).at("/template/mappings/properties/email/type").asText())
                .isEqualTo("text");

        manager.ensureResource(RESOURCE, EVOLVED_SCHEMA);
        assertThat(stub.requests("PUT", "/_component_template/" + TEMPLATE)).hasSize(2);

        manager.invalidate(RESOURCE);
        manager.ensureResource(RESOURCE, EVOLVED_SCHEMA);
        assertThat(stub.requests("PUT", "/_component_template/" + TEMPLATE)).as("invalidation forces the sequence again").hasSize(3);
    }

    @Test
    void shouldTrackResourcesIndependently() {
        final MappingManager manager = manager(Map.of());

        manager.ensureResource("a", SCHEMA);
        manager.ensureResource("b", SCHEMA);

        assertThat(stub.requests("PUT", "/a")).hasSize(1);
        assertThat(stub.requests("PUT", "/b")).hasSize(1);
        assertThat(stub.requests("PUT", "/_index_template/debezium-orders-sink-a")).hasSize(1);
        assertThat(stub.requests("PUT", "/_index_template/debezium-orders-sink-b")).hasSize(1);
    }

    @Test
    void shouldKeepTheExistingIndexTemplateUnderCreateIfAbsentButStillRefreshTheComponentTemplate() {
        stub.enqueue("HEAD", "/_index_template/" + TEMPLATE, StubResponse.status(200, ""));

        manager(Map.of()).ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests("PUT", "/_component_template/" + TEMPLATE)).hasSize(1);
        assertThat(stub.requests("PUT", "/_index_template/" + TEMPLATE)).isEmpty();
        assertThat(stub.requests("PUT", "/" + RESOURCE)).as("the index is still created").hasSize(1);
    }

    @Test
    void shouldAlwaysPutTheIndexTemplateUnderOverwrite() {
        stub.enqueue("HEAD", "/_index_template/" + TEMPLATE, StubResponse.status(200, ""));

        manager(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_MODE, "overwrite")).ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests("PUT", "/_index_template/" + TEMPLATE)).hasSize(1);
    }

    @Test
    void shouldNotCreateAnIndexThatAlreadyExists() {
        stub.enqueue("HEAD", "/" + RESOURCE, StubResponse.status(200, ""));

        manager(Map.of()).ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests("HEAD", "/" + RESOURCE)).hasSize(1);
        assertThat(stub.requests("PUT", "/" + RESOURCE)).isEmpty();
        assertThat(stub.requests("PUT", "/_index_template/" + TEMPLATE)).as("templates still apply for future indices").hasSize(1);
    }

    @Test
    void shouldLeaveMappingsToElasticsearchUnderMappingModeNoneButStillCreateTheIndex() {
        manager(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_MODE, "none")).ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests()).extracting(RecordedRequest::method, RecordedRequest::path).containsExactly(
                tuple("HEAD", "/" + RESOURCE),
                tuple("PUT", "/" + RESOURCE));
    }

    @Test
    void shouldMakeNoCallsAtAllWhenAutoCreateIsOff() {
        // DDD-61 4.2: 'resource.auto.create=false' means no existence call at all.
        final MappingManager manager = manager(Map.of(ElasticsearchSinkConnectorConfig.RESOURCE_AUTO_CREATE, "false"));

        manager.ensureResource(RESOURCE, SCHEMA);
        manager.ensureResource(RESOURCE, null);

        assertThat(stub.requests()).isEmpty();
    }

    @Test
    void shouldSkipTemplatesForASchemalessRecordButStillCreateTheIndex() {
        manager(Map.of()).ensureResource(RESOURCE, null);

        assertThat(stub.requests()).extracting(RecordedRequest::method, RecordedRequest::path).containsExactly(
                tuple("HEAD", "/" + RESOURCE),
                tuple("PUT", "/" + RESOURCE));
    }

    @Test
    void shouldPassThroughTheSupportedIndexSettings() throws IOException {
        manager(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_SETTINGS,
                "number_of_shards=3,number_of_replicas=1,refresh_interval=30s,index.default_pipeline=enrich"))
                .ensureResource(RESOURCE, SCHEMA);

        final JsonNode settings = body(stub.requests("PUT", "/_index_template/" + TEMPLATE).get(0)).at("/template/settings");
        assertThat(settings.at("/number_of_shards").asText()).isEqualTo("3");
        assertThat(settings.at("/number_of_replicas").asText()).isEqualTo("1");
        assertThat(settings.at("/refresh_interval").asText()).isEqualTo("30s");
        assertThat(settings.at("/default_pipeline").asText()).isEqualTo("enrich");
    }

    @Test
    void shouldRejectAnUnsupportedIndexSettingNamingTheKey() {
        // The narrow passthrough is enforced at configuration time, before any cluster call.
        assertThatThrownBy(() -> config(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_SETTINGS, "number_of_shards=3,index.codec=best_compression")))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'" + ElasticsearchSinkConnectorConfig.MAPPING_SETTINGS + "'")
                .hasMessageContaining("'index.codec'");
        assertThat(stub.requests()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "strict_but_dlq, strict", "true, true", "false, false", "runtime, runtime" })
    void shouldPropagateMappingDynamicIntoTheComponentTemplate(String configured, String expected) throws IOException {
        manager(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_DYNAMIC, configured)).ensureResource(RESOURCE, SCHEMA);

        final JsonNode component = body(stub.requests("PUT", "/_component_template/" + TEMPLATE).get(0));
        assertThat(component.at("/template/mappings/dynamic").asText()).isEqualTo(expected);
    }

    @Test
    void shouldSanitizeTheConnectorNameIntoTheTemplateName() {
        manager(Map.of(), "My Sink/Name").ensureResource(RESOURCE, SCHEMA);

        assertThat(stub.requests("PUT", "/_component_template/debezium-my-sink-name-" + RESOURCE)).hasSize(1);
        assertThat(stub.requests("PUT", "/_index_template/debezium-my-sink-name-" + RESOURCE)).hasSize(1);
    }

    @Test
    void shouldDiagnoseAMappingConflictAsAReindexProblem() {
        stub.enqueue("PUT", "/" + RESOURCE, StubResponse.error(400, "illegal_argument_exception",
                "mapper [name] cannot be changed from type [long] to [text]"));

        assertThatThrownBy(() -> manager(Map.of()).ensureResource(RESOURCE, SCHEMA))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("conflicts with the existing one")
                .hasMessageContaining("mapper [name] cannot be changed")
                .hasMessageContaining("reindexing is the remedy");
    }

    @Test
    void shouldDiagnoseATemplateConflictTheSameWay() {
        stub.enqueue("PUT", "/_index_template/" + TEMPLATE, StubResponse.error(400, "illegal_argument_exception",
                "composable template [" + TEMPLATE + "] template after composition is invalid: mapping conflict"));

        assertThatThrownBy(() -> manager(Map.of()).ensureResource(RESOURCE, SCHEMA))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("conflicts with the existing one")
                .hasMessageContaining("reindexing is the remedy");
        assertThat(stub.requests("PUT", "/" + RESOURCE)).as("the sequence stops at the failure").isEmpty();
    }

    @Test
    void shouldReportOtherClusterRejectionsWithTheirReason() {
        stub.enqueue("PUT", "/_component_template/" + TEMPLATE, StubResponse.error(403, "security_exception",
                "action [cluster:admin/component_template/put] is unauthorized"));

        assertThatThrownBy(() -> manager(Map.of()).ensureResource(RESOURCE, SCHEMA))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Failed to prepare resource '" + RESOURCE + "'")
                .hasMessageContaining("is unauthorized")
                .hasMessageNotContaining("reindexing");
    }

    @Test
    void shouldNotMemoizeAFailedAttempt() {
        stub.enqueue("PUT", "/_component_template/" + TEMPLATE, StubResponse.dropConnection());
        final MappingManager manager = manager(Map.of());

        assertThatThrownBy(() -> manager.ensureResource(RESOURCE, SCHEMA))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Failed to prepare resource");

        manager.ensureResource(RESOURCE, SCHEMA);
        assertThat(stub.requests("PUT", "/" + RESOURCE)).as("the retry completes the sequence").hasSize(1);
    }

    private MappingManager manager(Map<String, String> overrides) {
        return manager(overrides, "orders-sink");
    }

    private MappingManager manager(Map<String, String> overrides, String connectorName) {
        return new MappingManager(stub.client(), config(overrides), connectorName);
    }

    private ElasticsearchSinkConnectorConfig config(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url());
        properties.putAll(overrides);
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(properties);
        config.validate();
        return config;
    }

    private static JsonNode body(RecordedRequest request) {
        try {
            return JSON.readTree(request.body());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Recorded body is not JSON: " + request.body(), e);
        }
    }

    private static org.assertj.core.groups.Tuple tuple(Object... values) {
        return org.assertj.core.groups.Tuple.tuple(values);
    }
}
