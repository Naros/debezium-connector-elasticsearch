/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.elasticsearch.util.Backoff;
import io.debezium.util.DelayStrategy;

/**
 * The startup cluster-version handshake and its failure contract. No code path may turn a
 * missing probe result into a running task: an unsupported version fails naming the version and
 * the supported range; a probe that fails to answer retries within the configured budget and
 * then fails with the transport error attached; only a successful result is memoized, and a
 * version-shaped error later on invalidates the cache and forces a re-probe.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 10.1.1"
 */
public class ClusterVersionHandshake {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionHandshake.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The client line this connector compiles against; 9.x clusters are reached via compatibility headers. */
    public static final int CLIENT_MAJOR = 8;
    private static final int MIN_SUPPORTED_MAJOR = 8;
    private static final int MAX_SUPPORTED_MAJOR = 9;

    /**
     * A successfully probed cluster identity.
     */
    public record ClusterVersion(String number, int major, String distribution) {
    }

    private final RestClient restClient;
    private final int maxRetries;
    private final Supplier<DelayStrategy> backoffs;
    private final AtomicReference<ClusterVersion> probed = new AtomicReference<>();

    public ClusterVersionHandshake(RestClient restClient, int maxRetries, long retryBackoffMs, long retryBackoffMaxMs) {
        this(restClient, maxRetries, () -> Backoff.exponential(retryBackoffMs, retryBackoffMaxMs));
    }

    /**
     * The supplier yields the backoff between the attempts of one probe; injectable for tests.
     */
    public ClusterVersionHandshake(RestClient restClient, int maxRetries, Supplier<DelayStrategy> backoffs) {
        this.restClient = restClient;
        this.maxRetries = maxRetries;
        this.backoffs = backoffs;
    }

    /**
     * Returns the memoized cluster version, probing when none is cached. Never caches a failure.
     */
    public ClusterVersion ensureProbed() {
        final ClusterVersion cached = probed.get();
        if (cached != null) {
            return cached;
        }
        final ClusterVersion version = probeWithRetry();
        probed.set(version);
        LOGGER.info("Connected to Elasticsearch cluster version {} (client major {}, compatibility headers {}).",
                version.number(), CLIENT_MAJOR, version.major() > CLIENT_MAJOR ? "in effect" : "not required");
        return version;
    }

    /**
     * Invalidates the cached result after a version-shaped error (a media-type or compatibility
     * rejection on a later request), so the next request re-probes. Covers a cluster upgraded
     * underneath a long-running task.
     */
    public void invalidate() {
        probed.set(null);
    }

    private ClusterVersion probeWithRetry() {
        IOException lastFailure = null;
        final DelayStrategy backoff = backoffs.get();
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return validate(probeOnce());
            }
            catch (IOException e) {
                lastFailure = e;
                LOGGER.warn("Cluster version probe attempt {}/{} failed: {}", attempt + 1, maxRetries + 1, e.getMessage());
                if (attempt == maxRetries) {
                    break;
                }
                backoff.sleepWhen(true);
                if (Thread.currentThread().isInterrupted()) {
                    throw new ConnectException("Interrupted while probing the Elasticsearch cluster version");
                }
            }
        }
        // There is no fallback to an assumed compatibility mode: the retry budget is exhausted,
        // so the task fails with the transport error attached.
        throw new ConnectException(String.format(
                "The Elasticsearch cluster version probe failed %d times and the retry budget ('max.retries') is exhausted; "
                        + "the connector will not start with an assumed compatibility mode.",
                maxRetries + 1), lastFailure);
    }

    private ClusterVersion probeOnce() throws IOException {
        final Response response = restClient.performRequest(new Request("GET", "/"));
        final JsonNode root = JSON.readTree(response.getEntity().getContent());
        final JsonNode version = root.path("version");
        final String number = version.path("number").asText(null);
        if (number == null) {
            throw new ConnectException(String.format(
                    "The endpoint did not answer the root API with an Elasticsearch version document; it does not appear "
                            + "to be an Elasticsearch cluster (response: %s).",
                    root.toString()));
        }
        final String distribution = version.path("distribution").asText("elasticsearch");
        final int major;
        try {
            major = Integer.parseInt(number.split("\\.", 2)[0]);
        }
        catch (NumberFormatException e) {
            throw new ConnectException(String.format(
                    "The endpoint reports version '%s', which is not a parseable Elasticsearch version; it does not appear "
                            + "to be an Elasticsearch cluster.",
                    number));
        }
        return new ClusterVersion(number, major, distribution);
    }

    private ClusterVersion validate(ClusterVersion version) {
        if (!"elasticsearch".equalsIgnoreCase(version.distribution())) {
            throw new ConnectException(String.format(
                    "The endpoint reports distribution '%s' version %s, which is not Elasticsearch. OpenSearch and other "
                            + "forks are not supported by this connector.",
                    version.distribution(), version.number()));
        }
        if (version.major() < MIN_SUPPORTED_MAJOR || version.major() > MAX_SUPPORTED_MAJOR) {
            throw new ConnectException(String.format(
                    "Elasticsearch cluster version %s is not supported; supported majors are %d.x through %d.x.",
                    version.number(), MIN_SUPPORTED_MAJOR, MAX_SUPPORTED_MAJOR));
        }
        return version;
    }
}
