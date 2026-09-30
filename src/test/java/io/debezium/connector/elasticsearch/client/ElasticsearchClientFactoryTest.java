/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Unit tests for the pure helpers of {@link ElasticsearchClientFactory}. Client construction
 * itself, with TLS, proxy, and sniffer wiring, is covered by integration tests.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ElasticsearchClientFactoryTest {

    @Test
    void shouldDecodeCloudIdWithDefaultPort() {
        final HttpHost host = ElasticsearchClientFactory.decodeCloudId(cloudId("us-east-1.aws.found.io", "abc123", "kibana456"));
        assertThat(host.getHostName()).isEqualTo("abc123.us-east-1.aws.found.io");
        assertThat(host.getPort()).isEqualTo(443);
        assertThat(host.getSchemeName()).isEqualTo("https");
    }

    @Test
    void shouldDecodeCloudIdWithExplicitPort() {
        final HttpHost host = ElasticsearchClientFactory.decodeCloudId(cloudId("cloud.example.com:9243", "esuuid", "kbuuid"));
        assertThat(host.getHostName()).isEqualTo("esuuid.cloud.example.com");
        assertThat(host.getPort()).isEqualTo(9243);
        assertThat(host.getSchemeName()).isEqualTo("https");
    }

    @Test
    void shouldDecodeCloudIdWithoutKibanaComponent() {
        final String data = Base64.getEncoder().encodeToString("cloud.example.com$esuuid".getBytes(StandardCharsets.UTF_8));
        final HttpHost host = ElasticsearchClientFactory.decodeCloudId("deployment:" + data);
        assertThat(host.getHostName()).isEqualTo("esuuid.cloud.example.com");
    }

    @Test
    void shouldRejectCloudIdWithoutLabel() {
        assertThatThrownBy(() -> ElasticsearchClientFactory.decodeCloudId("bm90LWEtY2xvdWQtaWQ="))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("<label>:<base64 data>");
    }

    @Test
    void shouldRejectCloudIdWhoseDataLacksTheEsComponent() {
        final String data = Base64.getEncoder().encodeToString("cloud.example.com".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> ElasticsearchClientFactory.decodeCloudId("deployment:" + data))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("host$es_uuid");
    }

    private static String cloudId(String hostAndPort, String esId, String kibanaId) {
        final String data = hostAndPort + "$" + esId + "$" + kibanaId;
        return "my-deployment:" + Base64.getEncoder().encodeToString(data.getBytes(StandardCharsets.UTF_8));
    }
}
