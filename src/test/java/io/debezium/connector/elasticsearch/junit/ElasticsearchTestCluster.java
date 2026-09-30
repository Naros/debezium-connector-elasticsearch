/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.junit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * A single Elasticsearch container shared by every integration test in the JVM, with basic
 * authentication enabled and TLS disabled so the connector's credential path is exercised without
 * certificate provisioning. The image is selected with the {@code elasticsearch.image} system
 * property, which is how the 8.x and 9.x matrix is run.
 *
 * @author Chris Cranford
 */
public final class ElasticsearchTestCluster {

    public static final String IMAGE_PROPERTY = "elasticsearch.image";
    public static final String DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.19.20";

    private static final String USERNAME = "elastic";
    private static final String PASSWORD = "debezium";

    private static ElasticsearchTestCluster instance;

    private final ElasticsearchContainer container;
    private final RestClient restClient;
    private final ElasticsearchClient client;

    private ElasticsearchTestCluster() {
        final String image = System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
        container = new ElasticsearchContainer(DockerImageName.parse(image)
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"))
                .withPassword(PASSWORD)
                .withEnv("xpack.security.http.ssl.enabled", "false")
                .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        container.start();

        final BasicCredentialsProvider credentials = new BasicCredentialsProvider();
        credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));
        restClient = RestClient.builder(HttpHost.create(url()))
                .setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(credentials))
                .build();
        client = new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    /**
     * Returns the shared cluster, starting it on first use. The container is stopped by
     * Testcontainers' resource reaper when the JVM exits.
     */
    public static synchronized ElasticsearchTestCluster get() {
        if (instance == null) {
            instance = new ElasticsearchTestCluster();
        }
        return instance;
    }

    public String url() {
        return "http://" + container.getHttpHostAddress();
    }

    public String username() {
        return USERNAME;
    }

    public String password() {
        return PASSWORD;
    }

    /**
     * A client for assertions, independent of the one the connector builds.
     */
    public ElasticsearchClient client() {
        return client;
    }

    /**
     * The connection properties the connector needs to reach this cluster.
     */
    public Map<String, String> connectionProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL, url());
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_AUTH_MODE, "basic");
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME, username());
        properties.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD, password());
        return properties;
    }

    /**
     * Removes every index, index template, and component template the connector may have created
     * for the given resource names, so each test starts from an empty cluster.
     */
    public void reset(List<String> resources) throws IOException {
        for (String resource : resources) {
            client.indices().delete(d -> d.index(resource).ignoreUnavailable(true));
        }
        final List<String> indexTemplates = client.indices().getIndexTemplate(g -> g.name("debezium-*")).indexTemplates()
                .stream().map(t -> t.name()).toList();
        for (String template : indexTemplates) {
            client.indices().deleteIndexTemplate(d -> d.name(template));
        }
        final List<String> componentTemplates = client.cluster().getComponentTemplate(g -> g.name("debezium-*")).componentTemplates()
                .stream().map(t -> t.name()).toList();
        for (String template : componentTemplates) {
            client.cluster().deleteComponentTemplate(d -> d.name(template));
        }
    }
}
