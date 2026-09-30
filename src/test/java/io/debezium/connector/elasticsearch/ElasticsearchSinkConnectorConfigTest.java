/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.BULK_SIZE_BYTES;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_API_KEY;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_AUTH_MODE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_CLOUD_ID;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_KERBEROS_KEYTAB;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_KERBEROS_PRINCIPAL;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_URL;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.DOCUMENT_ID_NON_KEY_ORDERING;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.MAPPING_MODE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.MAX_REQUESTS_PER_SECOND;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.RESOURCE_AUTO_CREATE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.RESOURCE_TYPE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MAX_MS;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOW_WILDCARD;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TRUNCATE_MODE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.VERSION_CONFLICT_MODE;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.VERSION_ENFORCEMENT;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.VERSION_STRATEGY;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD;
import static io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.WRITE_METHOD_PER_OPERATION;
import static io.debezium.sink.SinkConnectorConfig.DELETE_ENABLED;
import static io.debezium.sink.SinkConnectorConfig.KEYED_MESSAGE_BATCH_MODE;
import static io.debezium.sink.SinkConnectorConfig.PRIMARY_KEY_FIELDS;
import static io.debezium.sink.SinkConnectorConfig.PRIMARY_KEY_MODE;
import static io.debezium.sink.SinkConnectorConfig.TRUNCATE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.TruncateMode;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.debezium.sink.SinkConnectorConfig.PrimaryKeyMode;

/**
 * The cross-property validations of DDD-61 implementation step 1, and their mirror image: a
 * configuration that names a single documented feature must start, because a validation suite
 * that only asserts failures will happily pass a connector no user can configure.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchSinkConnectorConfigTest {

    private static final String URL = "http://localhost:9200";

    static Stream<Arguments> rejectedCombinations() {
        return Stream.of(
                Arguments.of("no connection at all", Map.of(), new String[]{ CONNECTION_URL, CONNECTION_CLOUD_ID }),
                Arguments.of("url and cloud id together", Map.of(CONNECTION_URL, URL, CONNECTION_CLOUD_ID, "x"),
                        new String[]{ CONNECTION_URL, CONNECTION_CLOUD_ID }),
                Arguments.of("basic without credentials", with(CONNECTION_AUTH_MODE, "basic"),
                        new String[]{ CONNECTION_AUTH_MODE, CONNECTION_USERNAME, CONNECTION_PASSWORD }),
                Arguments.of("basic with only a username", with(CONNECTION_AUTH_MODE, "basic", CONNECTION_USERNAME, "u"),
                        new String[]{ CONNECTION_USERNAME, CONNECTION_PASSWORD }),
                Arguments.of("credentials that contradict the auth mode", with(CONNECTION_AUTH_MODE, "api_key", CONNECTION_USERNAME, "u",
                        CONNECTION_PASSWORD, "p"),
                        new String[]{ CONNECTION_AUTH_MODE, CONNECTION_USERNAME }),
                Arguments.of("api key mode without a key", with(CONNECTION_AUTH_MODE, "api_key"),
                        new String[]{ CONNECTION_AUTH_MODE, CONNECTION_API_KEY }),
                Arguments.of("kerberos without a keytab", with(CONNECTION_AUTH_MODE, "kerberos", CONNECTION_KERBEROS_PRINCIPAL, "p"),
                        new String[]{ CONNECTION_KERBEROS_PRINCIPAL, CONNECTION_KERBEROS_KEYTAB }),
                Arguments.of("no identity but deletes", with(PRIMARY_KEY_MODE, "none", DELETE_ENABLED, "true"),
                        new String[]{ PRIMARY_KEY_MODE, DELETE_ENABLED }),
                Arguments.of("no identity but upsert", with(PRIMARY_KEY_MODE, "none", WRITE_METHOD, "upsert"),
                        new String[]{ PRIMARY_KEY_MODE, WRITE_METHOD }),
                Arguments.of("no identity but update", with(PRIMARY_KEY_MODE, "none", WRITE_METHOD, "update"),
                        new String[]{ PRIMARY_KEY_MODE, WRITE_METHOD }),
                Arguments.of("no identity but a per-operation upsert",
                        with(PRIMARY_KEY_MODE, "none", WRITE_METHOD_PER_OPERATION, "u:upsert"),
                        new String[]{ PRIMARY_KEY_MODE, WRITE_METHOD_PER_OPERATION }),
                Arguments.of("record_value without fields", with(PRIMARY_KEY_MODE, "record_value"),
                        new String[]{ PRIMARY_KEY_MODE, PRIMARY_KEY_FIELDS }),
                Arguments.of("record_header without fields", with(PRIMARY_KEY_MODE, "record_header"),
                        new String[]{ PRIMARY_KEY_MODE, PRIMARY_KEY_FIELDS }),
                Arguments.of("non-key id without an ordering decision", with(PRIMARY_KEY_MODE, "record_value", PRIMARY_KEY_FIELDS, "id"),
                        new String[]{ PRIMARY_KEY_MODE, DOCUMENT_ID_NON_KEY_ORDERING }),
                Arguments.of("version ordering without a version strategy",
                        with(PRIMARY_KEY_MODE, "record_value", PRIMARY_KEY_FIELDS, "id", DOCUMENT_ID_NON_KEY_ORDERING, "version"),
                        new String[]{ DOCUMENT_ID_NON_KEY_ORDERING, VERSION_STRATEGY }),
                Arguments.of("external versioning with the update api", with(VERSION_ENFORCEMENT, "external"),
                        new String[]{ VERSION_ENFORCEMENT, WRITE_METHOD }),
                Arguments.of("passthrough batching", with(KEYED_MESSAGE_BATCH_MODE, "passthrough"),
                        new String[]{ KEYED_MESSAGE_BATCH_MODE }),
                Arguments.of("truncate.enabled contradicting the default mode", with(TRUNCATE_ENABLED, "true"),
                        new String[]{ TRUNCATE_ENABLED, TRUNCATE_MODE }),
                Arguments.of("truncate.enabled contradicting an explicit mode",
                        with(TRUNCATE_ENABLED, "false", TRUNCATE_MODE, "delete_by_query", TRUNCATE_ALLOWED_RESOURCES, "a"),
                        new String[]{ TRUNCATE_ENABLED, TRUNCATE_MODE }),
                Arguments.of("delete_by_query without an allow list", with(TRUNCATE_MODE, "delete_by_query"),
                        new String[]{ TRUNCATE_MODE, TRUNCATE_ALLOWED_RESOURCES }),
                Arguments.of("recreate without an allow list", with(TRUNCATE_MODE, "recreate"),
                        new String[]{ TRUNCATE_MODE, TRUNCATE_ALLOWED_RESOURCES }),
                Arguments.of("bare wildcard without acknowledgement",
                        with(TRUNCATE_MODE, "delete_by_query", TRUNCATE_ALLOWED_RESOURCES, "*"),
                        new String[]{ TRUNCATE_ALLOWED_RESOURCES, TRUNCATE_ALLOW_WILDCARD }),
                Arguments.of("double star without acknowledgement",
                        with(TRUNCATE_MODE, "delete_by_query", TRUNCATE_ALLOWED_RESOURCES, "safe,**"),
                        new String[]{ TRUNCATE_ALLOWED_RESOURCES, TRUNCATE_ALLOW_WILDCARD }),
                Arguments.of("recreate without auto create",
                        with(TRUNCATE_MODE, "recreate", TRUNCATE_ALLOWED_RESOURCES, "a", RESOURCE_AUTO_CREATE, "false"),
                        new String[]{ TRUNCATE_MODE, RESOURCE_AUTO_CREATE }),
                Arguments.of("recreate without a mapping",
                        with(TRUNCATE_MODE, "recreate", TRUNCATE_ALLOWED_RESOURCES, "a", MAPPING_MODE, "none"),
                        new String[]{ TRUNCATE_MODE, MAPPING_MODE }),
                Arguments.of("data stream with upsert", with(RESOURCE_TYPE, "data_stream", WRITE_METHOD, "upsert"),
                        new String[]{ RESOURCE_TYPE, WRITE_METHOD }),
                Arguments.of("data stream with deletes", with(RESOURCE_TYPE, "data_stream", DELETE_ENABLED, "true"),
                        new String[]{ RESOURCE_TYPE, DELETE_ENABLED }),
                Arguments.of("data stream with truncates",
                        with(RESOURCE_TYPE, "data_stream", TRUNCATE_MODE, "fail"),
                        new String[]{ RESOURCE_TYPE, TRUNCATE_MODE }),
                Arguments.of("data stream with a per-operation index",
                        with(RESOURCE_TYPE, "data_stream", WRITE_METHOD_PER_OPERATION, "c:index"),
                        new String[]{ RESOURCE_TYPE, WRITE_METHOD_PER_OPERATION }),
                Arguments.of("backoff above its ceiling", with(RETRY_BACKOFF_MS, "5000", RETRY_BACKOFF_MAX_MS, "1000"),
                        new String[]{ RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedCombinations")
    void shouldRejectCombinationNamingEveryProperty(String name, Map<String, String> properties, String[] expectedInMessage) {
        assertThatThrownBy(() -> new ElasticsearchSinkConnectorConfig(properties).validate())
                .isInstanceOf(ConnectException.class)
                .satisfies(e -> {
                    for (String property : expectedInMessage) {
                        assertThat(e.getMessage()).as("message names '%s'", property).contains("'" + property);
                    }
                });
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-2", "abc" })
    void shouldRejectInvalidBulkSizeBytes(String value) {
        assertThatThrownBy(() -> new ElasticsearchSinkConnectorConfig(with(BULK_SIZE_BYTES, value)).validate())
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldAcceptUnboundedBulkSizeBytes() {
        assertThatCode(() -> new ElasticsearchSinkConnectorConfig(with(BULK_SIZE_BYTES, "-1")).validate()).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-1", "nope" })
    void shouldRejectNonPositiveRequestRate(String value) {
        assertThatThrownBy(() -> new ElasticsearchSinkConnectorConfig(with(MAX_REQUESTS_PER_SECOND, value)).validate())
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void shouldRejectUnknownEnumeratedValueRatherThanFallingBackToTheDefault() {
        // A typo in a mode must not silently become the default: 'write.method=upsrt' resolving to
        // 'upsert' is exactly the quiet regression the validation exists to prevent.
        assertThatThrownBy(() -> new ElasticsearchSinkConnectorConfig(with(WRITE_METHOD, "upsrt")).validate())
                .isInstanceOf(ConnectException.class);
    }

    static Stream<Arguments> notYetImplemented() {
        return Stream.of(
                Arguments.of("data streams (step 10)", with(RESOURCE_TYPE, "data_stream"), "step 10"),
                Arguments.of("version strategy (step 13)", with(VERSION_STRATEGY, "source_ts_ms"), "step 13"),
                Arguments.of("version enforcement (step 13)", with(VERSION_ENFORCEMENT, "script"), "step 13"),
                Arguments.of("version conflict mode (step 13)", with(VERSION_CONFLICT_MODE, "fail"), "step 13"),
                Arguments.of("flush timeout", with(FLUSH_TIMEOUT_MS, "1000"), "not yet honored"),
                Arguments.of("per-topic overrides (step 15)", with("topic.inventory.customers.write.method", "index"), "step 15"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("notYetImplemented")
    void shouldRejectNotYetImplementedFeatureNamingTheStep(String name, Map<String, String> properties, String expectedInMessage) {
        assertThatThrownBy(() -> new ElasticsearchSinkConnectorConfig(properties).validate())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining(expectedInMessage);
    }

    @Test
    void shouldNotMistakeTopicToResourceMappingForAPerTopicOverride() {
        assertThatCode(() -> new ElasticsearchSinkConnectorConfig(
                with(ElasticsearchSinkConnectorConfig.TOPIC_TO_RESOURCE_MAPPING, "inventory.customers:customers")).validate())
                .doesNotThrowAnyException();
    }

    static Stream<Arguments> singlePropertyFeatures() {
        return Stream.of(
                Arguments.of("nothing but the connection", with()),
                Arguments.of("basic auth", with(CONNECTION_USERNAME, "u", CONNECTION_PASSWORD, "p")),
                Arguments.of("api key auth", with(CONNECTION_AUTH_MODE, "api_key", CONNECTION_API_KEY, "k")),
                Arguments.of("primary.key.mode=none", with(PRIMARY_KEY_MODE, "none")),
                Arguments.of("primary.key.mode=kafka", with(PRIMARY_KEY_MODE, "kafka")),
                Arguments.of("primary.key.mode=record_key", with(PRIMARY_KEY_MODE, "record_key")),
                Arguments.of("primary.key.mode=record_value with an ordering decision",
                        with(PRIMARY_KEY_MODE, "record_value", PRIMARY_KEY_FIELDS, "id", DOCUMENT_ID_NON_KEY_ORDERING, "last_write_wins")),
                Arguments.of("primary.key.mode=record_header with an ordering decision",
                        with(PRIMARY_KEY_MODE, "record_header", PRIMARY_KEY_FIELDS, "id", DOCUMENT_ID_NON_KEY_ORDERING, "last_write_wins")),
                Arguments.of("write.method=index", with(WRITE_METHOD, "index")),
                Arguments.of("write.method=create", with(WRITE_METHOD, "create")),
                Arguments.of("write.method=upsert", with(WRITE_METHOD, "upsert")),
                Arguments.of("write.method=update", with(WRITE_METHOD, "update")),
                Arguments.of("write.method.per.operation", with(WRITE_METHOD_PER_OPERATION, "c:create,u:update,r:index")),
                Arguments.of("truncate.mode=ignore", with(TRUNCATE_MODE, "ignore")),
                Arguments.of("truncate.mode=fail", with(TRUNCATE_MODE, "fail")),
                Arguments.of("truncate.mode=delete_by_query", with(TRUNCATE_MODE, "delete_by_query", TRUNCATE_ALLOWED_RESOURCES, "inventory.*")),
                Arguments.of("truncate.mode=recreate", with(TRUNCATE_MODE, "recreate", TRUNCATE_ALLOWED_RESOURCES, "inventory.*")),
                Arguments.of("acknowledged wildcard",
                        with(TRUNCATE_MODE, "delete_by_query", TRUNCATE_ALLOWED_RESOURCES, "*", TRUNCATE_ALLOW_WILDCARD, "true")),
                Arguments.of("truncate.enabled agreeing with the mode", with(TRUNCATE_ENABLED, "true", TRUNCATE_MODE, "fail")),
                Arguments.of("keyed.message.batch.mode=deduplication", with(KEYED_MESSAGE_BATCH_MODE, "deduplication")),
                Arguments.of("delete.enabled=false", with(DELETE_ENABLED, "false")),
                Arguments.of("resource.type=index", with(RESOURCE_TYPE, "index")),
                Arguments.of("resource.auto.create=false", with(RESOURCE_AUTO_CREATE, "false")),
                Arguments.of("mapping.mode=none", with(MAPPING_MODE, "none")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePropertyFeatures")
    void shouldStartWithASingleDocumentedFeature(String name, Map<String, String> properties) {
        assertThatCode(() -> new ElasticsearchSinkConnectorConfig(properties).validate()).doesNotThrowAnyException();
    }

    @Test
    void shouldDefaultDeleteEnabledToTrue() {
        assertThat(config(with()).isDeleteEnabled()).isTrue();
    }

    @Test
    void shouldResolveDefaultsAwayFromIdentityWhenThereIsNone() {
        final ElasticsearchSinkConnectorConfig config = config(with(PRIMARY_KEY_MODE, "none"));
        assertThat(config.getPrimaryKeyMode()).isEqualTo(PrimaryKeyMode.NONE);
        assertThat(config.writeMethod()).isEqualTo(WriteMethod.CREATE);
        assertThat(config.isDeleteEnabled()).isFalse();
    }

    @Test
    void shouldKeepExplicitWriteMethodWhenIdentityExists() {
        assertThat(config(with(WRITE_METHOD, "index")).writeMethod()).isEqualTo(WriteMethod.INDEX);
        assertThat(config(with()).writeMethod()).isEqualTo(WriteMethod.UPSERT);
    }

    @Test
    void shouldDeriveTruncateEnabledFromTruncateMode() {
        assertThat(config(with()).isTruncateEnabled()).isFalse();
        assertThat(config(with(TRUNCATE_MODE, "fail")).isTruncateEnabled()).isTrue();
        assertThat(config(with(TRUNCATE_MODE, "fail")).truncateMode()).isEqualTo(TruncateMode.FAIL);
    }

    @Test
    void shouldInferBasicAuthFromCredentialsWhenModeIsUnset() {
        assertThat(config(with(CONNECTION_USERNAME, "u", CONNECTION_PASSWORD, "p")).authMode())
                .isEqualTo(ElasticsearchSinkConnectorConfig.AuthMode.BASIC);
        assertThat(config(with()).authMode()).isEqualTo(ElasticsearchSinkConnectorConfig.AuthMode.NONE);
    }

    @Test
    void shouldKeepPrimaryKeyFieldsInConfiguredOrderAndTrimmed() {
        assertThat(config(with(PRIMARY_KEY_FIELDS, " tenant , id,,region ")).getPrimaryKeyFields())
                .containsExactly("tenant", "id", "region");
        assertThat(config(with(PRIMARY_KEY_FIELDS, "id,tenant")).getPrimaryKeyFields()).containsExactly("id", "tenant");
    }

    @Test
    void shouldScopeTemplatesByConnectorName() {
        assertThat(config(with("name", "orders-sink")).getConnectorName()).isEqualTo("orders-sink");
        assertThat(config(with()).getConnectorName()).isEqualTo(Module.name());
    }

    @Test
    void shouldParsePerOperationWriteMethods() {
        final ElasticsearchSinkConnectorConfig config = config(with(WRITE_METHOD_PER_OPERATION, "c:create,u:update"));
        assertThat(config.writeMethodFor(io.debezium.data.Envelope.Operation.CREATE)).isEqualTo(WriteMethod.CREATE);
        assertThat(config.writeMethodFor(io.debezium.data.Envelope.Operation.UPDATE)).isEqualTo(WriteMethod.UPDATE);
        assertThat(config.writeMethodFor(io.debezium.data.Envelope.Operation.READ)).isEqualTo(WriteMethod.UPSERT);
        assertThat(config.writeMethodFor(null)).isEqualTo(WriteMethod.UPSERT);
    }

    private static ElasticsearchSinkConnectorConfig config(Map<String, String> properties) {
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(properties);
        config.validate();
        return config;
    }

    /**
     * The connection details plus the given key/value pairs.
     */
    private static Map<String, String> with(String... keyValues) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTION_URL, URL);
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }
}
