/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.elasticsearch.client.ClusterVersionHandshake.ClusterVersion;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;
import io.debezium.connector.elasticsearch.util.StubElasticsearch.StubResponse;

/**
 * The startup handshake and its failure contract (DDD-61 10.1.1): transient failures are
 * retried and then fail the task with the transport error, nothing starts with an assumed
 * compatibility mode, failures are never cached, and unsupported clusters are named.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ClusterVersionHandshakeTest {

    private int pauses;
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
    void shouldProbeOnceAndMemoize() {
        final ClusterVersionHandshake handshake = handshake(0);

        final ClusterVersion first = handshake.ensureProbed();
        final ClusterVersion second = handshake.ensureProbed();

        assertThat(first).isEqualTo(new ClusterVersion("8.19.20", 8, "elasticsearch"));
        assertThat(second).isSameAs(first);
        assertThat(stub.requests("GET", "/")).hasSize(1);
    }

    @Test
    void shouldAcceptANinePointClusterThroughCompatibilityHeaders() {
        stub.withVersion("9.5.1");

        assertThat(handshake(0).ensureProbed().major()).isEqualTo(9);
    }

    @Test
    void shouldRejectAnUnsupportedMajorNamingTheVersionAndTheRange() {
        stub.withVersion("7.17.0");

        assertThatThrownBy(() -> handshake(3).ensureProbed())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("7.17.0")
                .hasMessageContaining("8.x through 9.x");
        assertThat(stub.requests("GET", "/")).as("a version rejection is not retried").hasSize(1);
    }

    @Test
    void shouldRejectAForkThatIsNotElasticsearch() {
        stub.withVersion("2.11.0").withDistribution("opensearch");

        assertThatThrownBy(() -> handshake(0).ensureProbed())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("opensearch")
                .hasMessageContaining("not Elasticsearch");
    }

    @Test
    void shouldRejectAnEndpointWithoutAVersionDocument() {
        stub.enqueue("GET", "/", StubResponse.ok("{\"hello\":\"world\"}"));

        assertThatThrownBy(() -> handshake(3).ensureProbed())
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("does not appear to be an Elasticsearch cluster");
        assertThat(stub.requests("GET", "/")).hasSize(1);
    }

    @Test
    void shouldRetryATransientFailureWithBackoffAndThenSucceed() {
        stub.enqueue("GET", "/", StubResponse.dropConnection());
        stub.enqueue("GET", "/", StubResponse.dropConnection());

        assertThat(handshake(3).ensureProbed().number()).isEqualTo("8.19.20");

        assertThat(stub.requests("GET", "/")).hasSize(3);
        assertThat(pauses).as("one backoff between each attempt").isEqualTo(2);
    }

    @Test
    void shouldFailTheTaskWithTheTransportErrorWhenRetriesAreExhaustedAndNeverCacheTheFailure() {
        stub.enqueue("GET", "/", StubResponse.dropConnection());
        stub.enqueue("GET", "/", StubResponse.dropConnection());
        stub.enqueue("GET", "/", StubResponse.dropConnection());
        final ClusterVersionHandshake handshake = handshake(2);

        assertThatThrownBy(handshake::ensureProbed)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("failed 3 times")
                .hasMessageContaining("will not start with an assumed compatibility mode")
                .cause().isInstanceOf(IOException.class);
        assertThat(pauses).as("one backoff between each attempt").isEqualTo(2);

        // The cluster recovers: the next call probes again rather than replaying the failure.
        assertThat(handshake.ensureProbed().number()).isEqualTo("8.19.20");
        assertThat(stub.requests("GET", "/")).hasSize(4);
    }

    @Test
    void shouldReprobeAfterInvalidation() {
        final ClusterVersionHandshake handshake = handshake(0);
        handshake.ensureProbed();
        stub.withVersion("9.5.1");

        assertThat(handshake.ensureProbed().number()).as("still cached").isEqualTo("8.19.20");
        handshake.invalidate();
        assertThat(handshake.ensureProbed().number()).isEqualTo("9.5.1");
        assertThat(stub.requests("GET", "/")).hasSize(2);
    }

    private ClusterVersionHandshake handshake(int maxRetries) {
        return new ClusterVersionHandshake(stub.restClient(), maxRetries, () -> criteria -> {
            if (criteria) {
                pauses++;
            }
            return criteria;
        });
    }
}
