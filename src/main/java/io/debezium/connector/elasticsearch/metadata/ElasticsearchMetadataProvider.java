/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.metadata;

import java.util.List;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnector;
import io.debezium.connector.elasticsearch.Module;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.metadata.ComponentMetadataProvider;

/**
 * Aggregator for all Elasticsearch connector metadata.
 */
public class ElasticsearchMetadataProvider implements ComponentMetadataProvider {

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(
                componentMetadataFactory.createComponentMetadata(new ElasticsearchSinkConnector(), "Debezium Elasticsearch Sink Connector",
                        "Writes change events to Elasticsearch indices.", Module.version()));
    }
}
