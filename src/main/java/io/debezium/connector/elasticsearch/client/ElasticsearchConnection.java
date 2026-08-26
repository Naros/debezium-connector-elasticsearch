/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;

/**
 * The live connection bundle: the low-level REST client, the typed client built on it, the
 * version handshake, and the optional node sniffer, closed together in reverse order.
 *
 * @author Chris Cranford
 */
public class ElasticsearchConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConnection.class);

    private final RestClient restClient;
    private final ElasticsearchTransport transport;
    private final ElasticsearchClient client;
    private final ClusterVersionHandshake handshake;
    private final Sniffer sniffer;

    ElasticsearchConnection(RestClient restClient, ElasticsearchTransport transport, ElasticsearchClient client,
                            ClusterVersionHandshake handshake, Sniffer sniffer) {
        this.restClient = restClient;
        this.transport = transport;
        this.client = client;
        this.handshake = handshake;
        this.sniffer = sniffer;
    }

    public ElasticsearchClient client() {
        return client;
    }

    public RestClient restClient() {
        return restClient;
    }

    public ClusterVersionHandshake handshake() {
        return handshake;
    }

    @Override
    public void close() {
        if (sniffer != null) {
            try {
                sniffer.close();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to close the node sniffer", e);
            }
        }
        try {
            transport.close();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to close the Elasticsearch transport", e);
        }
    }
}
