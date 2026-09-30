/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.RecordedRequest;

/**
 * The connection details DDD-61 Testing lists as regressing quietly, asserted on the wire
 * through a client the factory built: request compression, each authentication mode's header,
 * static headers, a context path on the connection URL, and URL validation.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchClientFactoryStubTest {

    private StubElasticsearch stub;
    private ElasticsearchConnection connection;

    @BeforeEach
    void beforeEach() throws IOException {
        stub = new StubElasticsearch();
    }

    @AfterEach
    void afterEach() throws IOException {
        if (connection != null) {
            connection.close();
        }
        stub.close();
    }

    @Test
    void shouldProbeTheClusterThroughTheFactoryBuiltConnection() {
        stub.withVersion("9.5.1");
        connection = connect(Map.of());

        assertThat(connection.handshake().ensureProbed().number()).isEqualTo("9.5.1");
        assertThat(stub.requests("GET", "/")).hasSize(1);
    }

    @Test
    void shouldCompressTheRequestBodyWhenEnabled() throws IOException {
        connection = connect(Map.of(ElasticsearchSinkConnectorConfig.CONNECTION_COMPRESSION, "true"));

        bulk();

        final RecordedRequest request = stub.requests("POST", "/_bulk").get(0);
        assertThat(request.headers()).containsKey("content-encoding");
        assertThat(request.headers().get("content-encoding")).containsExactly("gzip");
        assertThat(request.bulkActions()).singleElement().satisfies(action -> {
            assertThat(action.index()).isEqualTo("a");
            assertThat(action.id()).isEqualTo("1");
            assertThat(action.source()).contains("\"k\":\"v\"");
        });
    }

    @Test
    void shouldNotCompressByDefault() throws IOException {
        connection = connect(Map.of());

        bulk();

        assertThat(stub.requests("POST", "/_bulk").get(0).headers()).doesNotContainKey("content-encoding");
    }

    @Test
    void shouldSendBasicCredentialsPreemptively() {
        connection = connect(Map.of(
                ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME, "user",
                ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD, "pass"));

        connection.handshake().ensureProbed();

        final String authorization = authorization(stub.requests("GET", "/").get(0));
        assertThat(authorization).startsWith("Basic ");
        assertThat(new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())), StandardCharsets.UTF_8))
                .isEqualTo("user:pass");
    }

    @Test
    void shouldSendApiKeyHeader() {
        connection = connect(Map.of(
                ElasticsearchSinkConnectorConfig.CONNECTION_AUTH_MODE, "api_key",
                ElasticsearchSinkConnectorConfig.CONNECTION_API_KEY, "encoded-key"));

        connection.handshake().ensureProbed();

        assertThat(authorization(stub.requests("GET", "/").get(0))).isEqualTo("ApiKey encoded-key");
    }

    @Test
    void shouldSendBearerTokenHeader() {
        connection = connect(Map.of(
                ElasticsearchSinkConnectorConfig.CONNECTION_AUTH_MODE, "bearer",
                ElasticsearchSinkConnectorConfig.CONNECTION_BEARER_TOKEN, "tok"));

        connection.handshake().ensureProbed();

        assertThat(authorization(stub.requests("GET", "/").get(0))).isEqualTo("Bearer tok");
    }

    @Test
    void shouldSendNoAuthorizationWithoutCredentials() {
        connection = connect(Map.of());

        connection.handshake().ensureProbed();

        assertThat(stub.requests("GET", "/").get(0).headers()).doesNotContainKey("authorization");
    }

    @Test
    void shouldSendStaticHeadersOnEveryRequest() throws IOException {
        connection = connect(Map.of(ElasticsearchSinkConnectorConfig.CONNECTION_HEADERS, "X-Gateway:edge, X-Tenant:acme"));

        connection.handshake().ensureProbed();
        bulk();

        assertThat(stub.requests()).hasSize(2).allSatisfy(request -> {
            assertThat(request.headers().get("x-gateway")).containsExactly("edge");
            assertThat(request.headers().get("x-tenant")).containsExactly("acme");
        });
    }

    @Test
    void shouldHonorAContextPathOnTheConnectionUrl() throws IOException {
        stub.withPathPrefix("/es");
        connection = connect(Map.of(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url() + "/es"));

        connection.handshake().ensureProbed();
        bulk();

        assertThat(stub.requests()).extracting(RecordedRequest::path).containsExactly("/es/", "/es/_bulk");
    }

    @Test
    void shouldRejectUrlsWithDifferentContextPaths() {
        assertThatThrownBy(() -> connect(Map.of(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url() + "/es," + stub.url() + "/other")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("'connection.url'")
                .hasMessageContaining("'/es'")
                .hasMessageContaining("'/other'");
    }

    @Test
    void shouldRejectASchemelessUrlNamingIt() {
        assertThatThrownBy(() -> connect(Map.of(ElasticsearchSinkConnectorConfig.CONNECTION_URL, "localhost:9200")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("'localhost:9200'")
                .hasMessageContaining("http://localhost:9200");
    }

    @Test
    void shouldNotSniffNodesByDefault() throws IOException {
        // The default port (9200 for http, 443 for https) is only observable through a real
        // connection to that port, so it is not asserted here; the stub listens on an ephemeral port.
        connection = connect(Map.of());

        connection.handshake().ensureProbed();
        bulk();

        assertThat(stub.requests()).extracting(RecordedRequest::path).noneMatch(path -> path.startsWith("/_nodes"));
    }

    private ElasticsearchConnection connect(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, stub.url());
        properties.putAll(overrides);
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(properties);
        config.validate();
        return new ElasticsearchClientFactory(config, properties).connect();
    }

    private void bulk() throws IOException {
        connection.client().bulk(b -> b.operations(op -> op.index(i -> i.index("a").id("1").document(Map.of("k", "v")))));
    }

    private static String authorization(RecordedRequest request) {
        final List<String> values = request.headers().get("authorization");
        assertThat(values).as("authorization header").isNotNull().hasSize(1);
        return values.get(0);
    }
}
