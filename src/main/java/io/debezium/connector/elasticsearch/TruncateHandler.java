/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.elasticsearch.mapping.MappingManager;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.sink.DebeziumSinkRecord;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;

/**
 * Executes truncate ({@code t}) events per {@code truncate.mode}. The destructive modes run only
 * against resources matching the {@code truncate.allowed.resources} allow-list, matched on the
 * resolved resource name rather than the topic, and a truncate outside the list is a
 * record-level error rather than a silent no-op. The caller guarantees the sequencing: the
 * in-flight bulk is fully processed before this runs, and the next one is built only after it
 * completes.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 5.1"
 */
public class TruncateHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TruncateHandler.class);

    private final ElasticsearchClient client;
    private final ElasticsearchSinkConnectorConfig config;
    private final MappingManager mappingManager;
    private final List<Pattern> allowedPatterns;

    public TruncateHandler(ElasticsearchClient client, ElasticsearchSinkConnectorConfig config, MappingManager mappingManager) {
        this.client = client;
        this.config = config;
        this.mappingManager = mappingManager;
        this.allowedPatterns = config.truncateAllowedResources().stream()
                .map(TruncateHandler::globToPattern)
                .toList();
    }

    /**
     * Applies the truncate to the resolved resource.
     *
     * @throws RecordProcessingException when the resource is outside the allow-list
     * @throws ConnectException when the mode is {@code fail} or the truncate itself fails
     */
    public void truncate(DebeziumSinkRecord record, String resource) {
        switch (config.truncateMode()) {
            case IGNORE -> LOGGER.debug("Truncate for '{}' ignored.", resource);
            case FAIL -> throw new ConnectException(String.format(
                    "A truncate event arrived for resource '%s' and '%s' is 'fail'.",
                    resource, ElasticsearchSinkConnectorConfig.TRUNCATE_MODE));
            case DELETE_BY_QUERY -> {
                requireAllowed(record, resource);
                deleteByQuery(resource);
            }
            case RECREATE -> {
                requireAllowed(record, resource);
                recreate(resource);
            }
        }
    }

    private void requireAllowed(DebeziumSinkRecord record, String resource) {
        if (allowedPatterns.stream().noneMatch(pattern -> pattern.matcher(resource).matches())) {
            throw new RecordProcessingException(String.format(
                    "A truncate event for resolved resource '%s' (topic '%s' partition %s offset %s) does not match "
                            + "'%s' (%s); it is routed to the error handler rather than silently ignored.",
                    resource, record.topicName(), record.partition(), record.offset(),
                    ElasticsearchSinkConnectorConfig.TRUNCATE_ALLOWED_RESOURCES, config.truncateAllowedResources()));
        }
    }

    private void deleteByQuery(String resource) {
        final DeleteByQueryResponse response;
        try {
            // conflicts=abort is deliberate: a concurrent write during the scan fails the truncate
            // loudly instead of applying to part of the index and reporting success.
            // delete_by_query works from the search snapshot, so documents the drained bulk request
            // just indexed are invisible to it until the index is refreshed (DDD-61 5.1).
            client.indices().refresh(r -> r.index(resource));
            response = client.deleteByQuery(d -> d
                    .index(resource)
                    .query(q -> q.matchAll(m -> m))
                    .conflicts(Conflicts.Abort)
                    .refresh(true)
                    .waitForCompletion(true));
        }
        catch (Exception e) {
            throw new ConnectException(String.format("Truncate (delete_by_query) of resource '%s' failed", resource), e);
        }
        if (response.versionConflicts() != null && response.versionConflicts() > 0) {
            throw new ConnectException(String.format(
                    "Truncate of '%s' aborted on %d concurrent write conflicts; the truncate was not fully applied.",
                    resource, response.versionConflicts()));
        }
        LOGGER.info("Truncated resource '{}' via delete_by_query ({} documents deleted).", resource, response.deleted());
    }

    /**
     * Deletes the index outright and lets auto-create rebuild it from the generated template.
     * Where the connector exclusively owns the resource this is safer than delete_by_query: it is
     * atomic, has no conflict window, and reclaims disk immediately.
     */
    private void recreate(String resource) {
        try {
            client.indices().delete(d -> d.index(resource).ignoreUnavailable(true));
            mappingManager.invalidate(resource);
            LOGGER.info("Truncated resource '{}' by deletion; it will be recreated from the generated template on next write.",
                    resource);
        }
        catch (Exception e) {
            throw new ConnectException(String.format("Truncate (recreate) of resource '%s' failed", resource), e);
        }
    }

    private static Pattern globToPattern(String glob) {
        final StringBuilder regex = new StringBuilder();
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(regex.toString());
    }
}
