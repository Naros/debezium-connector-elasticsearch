/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.MappingMode;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.IndexSettings;

/**
 * Owns mapping and resource creation. When {@code resource.auto.create} is true the connector
 * creates the index and applies the generated mapping as one step, skipping both when the
 * resource exists; when false, no existence call is made at all. Mappings are carried by a
 * component template named {@code debezium-<connector>-<resource>} composed into a generated
 * index template, with user templates named in {@code mapping.composed.of} composed ahead of
 * the connector's own. Lifecycle (ILM, retention) is deliberately not owned here.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 4.2"
 * @see "DDD-61 Section 6.3"
 */
public class MappingManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappingManager.class);

    private final ElasticsearchClient client;
    private final ElasticsearchSinkConnectorConfig config;
    private final MappingGenerator generator;
    private final String connectorName;
    private final Map<String, Integer> ensuredSchemaHash = new ConcurrentHashMap<>();
    private final AtomicBoolean schemalessReported = new AtomicBoolean();

    public MappingManager(ElasticsearchClient client, ElasticsearchSinkConnectorConfig config, String connectorName) {
        this.client = client;
        this.config = config;
        this.generator = new MappingGenerator(config);
        this.connectorName = connectorName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_-]", "-");
    }

    /**
     * Ensures the resource exists with the generated mapping before its first write, memoized per
     * resource and value schema so that a schema change (a new column) regenerates the template.
     */
    public void ensureResource(String resource, Schema valueSchema) {
        if (!config.isResourceAutoCreate()) {
            return;
        }
        final int schemaHash = valueSchema != null ? valueSchema.hashCode() : 0;
        if (isEnsured(resource, schemaHash)) {
            return;
        }
        synchronized (this) {
            if (isEnsured(resource, schemaHash)) {
                return;
            }
            try {
                TypeMapping mapping = null;
                if (config.mappingMode() != MappingMode.NONE) {
                    if (valueSchema != null) {
                        mapping = applyTemplates(resource, valueSchema);
                    }
                    else if (schemalessReported.compareAndSet(false, true)) {
                        LOGGER.info("Mapping generation for resource '{}' skipped: the record value carries no schema, so "
                                + "'{}' has nothing to derive a mapping from.", resource, ElasticsearchSinkConnectorConfig.MAPPING_MODE);
                    }
                }
                if (!client.indices().exists(e -> e.index(resource)).value()) {
                    client.indices().create(c -> c.index(resource));
                    LOGGER.info("Created index '{}'.", resource);
                }
                else if (mapping != null) {
                    // Templates only shape indices created after them. An index that already
                    // exists, whether from an earlier schema or created by someone else, receives
                    // the generated fields directly: an added column is absorbed, and a type change
                    // is rejected by Elasticsearch here, once, rather than per document (DDD-61 6.3).
                    final TypeMapping generated = mapping;
                    client.indices().putMapping(m -> m.index(resource).dynamic(generated.dynamic()).properties(generated.properties()));
                    LOGGER.info("Applied the generated mapping to existing index '{}'.", resource);
                }
                ensuredSchemaHash.put(resource, schemaHash);
            }
            catch (ElasticsearchException e) {
                throw diagnose(resource, e);
            }
            catch (IOException e) {
                throw new ConnectException(String.format("Failed to prepare resource '%s'", resource), e);
            }
        }
    }

    private boolean isEnsured(String resource, int schemaHash) {
        final Integer known = ensuredSchemaHash.get(resource);
        return known != null && known == schemaHash;
    }

    /**
     * Forgets a resource after it has been deleted (truncate {@code recreate}), so the next write
     * re-creates it from the generated template.
     */
    public void invalidate(String resource) {
        ensuredSchemaHash.remove(resource);
    }

    private TypeMapping applyTemplates(String resource, Schema valueSchema) throws IOException {
        final String templateName = "debezium-" + connectorName + "-" + resource;
        final TypeMapping mapping = generator.generate(valueSchema);

        client.cluster().putComponentTemplate(c -> c
                .name(templateName)
                .template(t -> t.mappings(mapping)));

        final boolean indexTemplateExists = client.indices().existsIndexTemplate(e -> e.name(templateName)).value();
        if (config.mappingMode() == MappingMode.CREATE_IF_ABSENT && indexTemplateExists) {
            return mapping;
        }

        final List<String> composedOf = new ArrayList<>(config.mappingComposedOf());
        composedOf.add(templateName);
        final IndexSettings settings = indexSettings();
        client.indices().putIndexTemplate(t -> {
            t.name(templateName)
                    .indexPatterns(resource)
                    .composedOf(composedOf);
            if (settings != null) {
                t.template(tpl -> tpl.settings(settings));
            }
            return t;
        });
        LOGGER.info("Applied index template '{}' for resource '{}' (composed of {}).", templateName, resource, composedOf);
        return mapping;
    }

    private IndexSettings indexSettings() {
        final Map<String, String> configured = config.mappingSettings();
        if (configured.isEmpty()) {
            return null;
        }
        return IndexSettings.of(settings -> {
            configured.forEach((key, value) -> {
                switch (key) {
                    case "number_of_shards" -> settings.numberOfShards(value);
                    case "number_of_replicas" -> settings.numberOfReplicas(value);
                    case "refresh_interval" -> settings.refreshInterval(Time.of(t -> t.time(value)));
                    case "index.default_pipeline" -> settings.defaultPipeline(value);
                    default -> throw new ConnectException(String.format(
                            "Unsupported '%s' key '%s'.", ElasticsearchSinkConnectorConfig.MAPPING_SETTINGS, key));
                }
            });
            return settings;
        });
    }

    /**
     * A generated mapping conflicting with the existing one is reported once with the
     * Elasticsearch diagnostic, naming reindexing as the remedy, rather than surfacing as one raw
     * parse error per document forever.
     */
    private ConnectException diagnose(String resource, ElasticsearchException e) {
        final String reason = e.error() != null ? e.error().reason() : e.getMessage();
        if (reason != null && (reason.contains("mapper") || reason.contains("mapping"))) {
            return new ConnectException(String.format(
                    "The mapping generated for resource '%s' conflicts with the existing one: %s. Elasticsearch cannot "
                            + "change a field's type in place; reindexing is the remedy.",
                    resource, reason), e);
        }
        return new ConnectException(String.format("Failed to prepare resource '%s': %s", resource, reason), e);
    }
}
