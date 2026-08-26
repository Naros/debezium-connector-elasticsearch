/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.naming;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.InvalidNameHandling;
import io.debezium.data.Envelope;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.util.Strings;

/**
 * Resolves Elasticsearch resource names from the placeholder vocabulary: {@code ${topic}},
 * {@code ${source.db|schema|table|connector}}, {@code ${field:path}}, {@code ${header:name}},
 * and {@code ${date:pattern}}. The {@code topic.to.resource.mapping} override is consulted
 * before the format, unconditionally. An unresolvable placeholder is a record-level error,
 * never an empty string spliced into an index name, and a resolved name that breaks an
 * Elasticsearch naming rule is handled per {@code resource.name.invalid.handling}.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 4.1"
 */
public class ElasticsearchCollectionNamingStrategy implements CollectionNamingStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchCollectionNamingStrategy.class);

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");
    private static final Set<String> SOURCE_FIELDS = Set.of("db", "schema", "table", "connector");
    private static final String FLATTENED_SOURCE_PREFIX = "__source_";

    private Map<String, String> topicToResourceMapping = Map.of();
    private InvalidNameHandling invalidNameHandling = InvalidNameHandling.FAIL;
    private String replacement = "_";
    private ZoneId timezone = ZoneId.of("UTC");
    private final Map<String, DateTimeFormatter> formatters = new ConcurrentHashMap<>();
    private final Set<String> reportedSanitizations = ConcurrentHashMap.newKeySet();

    @Override
    public void configure(Map<String, String> properties) {
        final String mapping = properties.get(ElasticsearchSinkConnectorConfig.TOPIC_TO_RESOURCE_MAPPING);
        if (!Strings.isNullOrBlank(mapping)) {
            final Map<String, String> parsed = new LinkedHashMap<>();
            for (String entry : mapping.split(",")) {
                final String[] parts = entry.trim().split(":", 2);
                if (parts.length == 2) {
                    parsed.put(parts[0].trim(), parts[1].trim());
                }
            }
            this.topicToResourceMapping = parsed;
        }
        final String handling = properties.get(ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING);
        if (!Strings.isNullOrBlank(handling)) {
            this.invalidNameHandling = switch (handling.trim().toLowerCase(Locale.ROOT)) {
                case "sanitize" -> InvalidNameHandling.SANITIZE;
                case "error_handler" -> InvalidNameHandling.ERROR_HANDLER;
                default -> InvalidNameHandling.FAIL;
            };
        }
        this.replacement = properties.getOrDefault(ElasticsearchSinkConnectorConfig.RESOURCE_NAME_REPLACEMENT, "_");
        final String zone = properties.get(ElasticsearchSinkConnectorConfig.RESOURCE_NAME_TIMEZONE);
        if (!Strings.isNullOrBlank(zone)) {
            this.timezone = ZoneId.of(zone.trim());
        }
    }

    @Override
    public String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat) {
        final String override = topicToResourceMapping.get(record.topicName());
        final String resolved = override != null ? override : resolvePlaceholders(record, collectionNameFormat);
        return applyNameRules(record, resolved);
    }

    /**
     * Resolves the placeholder vocabulary in an arbitrary expression, e.g. an ingest pipeline
     * name, without applying index name rules.
     */
    public String resolveExpression(DebeziumSinkRecord record, String expression) {
        return resolvePlaceholders(record, expression);
    }

    private String resolvePlaceholders(DebeziumSinkRecord record, String format) {
        final Matcher matcher = PLACEHOLDER.matcher(format);
        final StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(result, Matcher.quoteReplacement(resolvePlaceholder(record, matcher.group(1))));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String resolvePlaceholder(DebeziumSinkRecord record, String expression) {
        if ("topic".equals(expression)) {
            return record.topicName();
        }
        if (expression.startsWith("source.")) {
            return resolveSourceField(record, expression.substring("source.".length()));
        }
        if (expression.startsWith("field:")) {
            return resolveFieldPath(record, expression.substring("field:".length()));
        }
        if (expression.startsWith("header:")) {
            return resolveHeader(record, expression.substring("header:".length()));
        }
        if (expression.startsWith("date:")) {
            return resolveDate(record, expression.substring("date:".length()));
        }
        throw new ResourceResolutionException(String.format(
                "Unknown placeholder '${%s}' in the resource name format for topic '%s'.", expression, record.topicName()));
    }

    private String resolveSourceField(DebeziumSinkRecord record, String field) {
        if (!SOURCE_FIELDS.contains(field)) {
            throw new ResourceResolutionException(String.format(
                    "Unknown source placeholder '${source.%s}'; supported: db, schema, table, connector.", field));
        }
        if (record.isDebeziumMessage() && record.value() instanceof Struct envelope) {
            final Struct source = envelope.getStruct(Envelope.FieldName.SOURCE);
            if (source != null && source.schema().field(field) != null) {
                final Object value = source.get(field);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        else {
            final Struct payload = record.getPayload();
            final String flattenedName = FLATTENED_SOURCE_PREFIX + field;
            if (payload != null && payload.schema().field(flattenedName) != null) {
                final Object value = payload.get(flattenedName);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        throw new ResourceResolutionException(String.format(
                "Placeholder '${source.%s}' cannot be resolved for record at topic '%s' partition %s offset %s: the record "
                        + "carries no such source metadata.",
                field, record.topicName(), record.partition(), record.offset()));
    }

    private String resolveFieldPath(DebeziumSinkRecord record, String path) {
        Object current = record.getPayload();
        for (String segment : path.split("\\.")) {
            if (current instanceof Struct struct && struct.schema().field(segment) != null) {
                current = struct.get(segment);
            }
            else if (current instanceof Map<?, ?> map && map.containsKey(segment)) {
                current = map.get(segment);
            }
            else {
                current = null;
            }
            if (current == null) {
                throw new ResourceResolutionException(String.format(
                        "Placeholder '${field:%s}' cannot be resolved for record at topic '%s' partition %s offset %s.",
                        path, record.topicName(), record.partition(), record.offset()));
            }
        }
        return current.toString();
    }

    private String resolveHeader(DebeziumSinkRecord record, String name) {
        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
            final Struct headers = kafkaRecord.kafkaHeader();
            if (headers != null && headers.schema().field(name) != null) {
                final Object value = headers.get(name);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        throw new ResourceResolutionException(String.format(
                "Placeholder '${header:%s}' cannot be resolved for record at topic '%s' partition %s offset %s: no such header.",
                name, record.topicName(), record.partition(), record.offset()));
    }

    private String resolveDate(DebeziumSinkRecord record, String pattern) {
        final DateTimeFormatter formatter = formatters.computeIfAbsent(pattern,
                p -> DateTimeFormatter.ofPattern(p).withZone(timezone));
        return formatter.format(Instant.ofEpochMilli(eventTimestamp(record)));
    }

    private long eventTimestamp(DebeziumSinkRecord record) {
        if (record.isDebeziumMessage() && record.value() instanceof Struct envelope) {
            final Struct source = envelope.getStruct(Envelope.FieldName.SOURCE);
            if (source != null && source.schema().field("ts_ms") != null) {
                final Object tsMs = source.get("ts_ms");
                if (tsMs instanceof Long ts) {
                    return ts;
                }
            }
        }
        if (record instanceof KafkaDebeziumSinkRecord kafkaRecord) {
            final Long timestamp = kafkaRecord.getOriginalKafkaRecord().timestamp();
            if (timestamp != null && timestamp > 0) {
                return timestamp;
            }
        }
        throw new ResourceResolutionException(String.format(
                "A '${date:...}' placeholder cannot be resolved for record at topic '%s' partition %s offset %s: the record "
                        + "carries no event timestamp.",
                record.topicName(), record.partition(), record.offset()));
    }

    private String applyNameRules(DebeziumSinkRecord record, String name) {
        return IndexNameValidator.violation(name)
                .map(violation -> switch (invalidNameHandling) {
                    case SANITIZE -> {
                        final String sanitized = IndexNameValidator.sanitize(name, replacement);
                        IndexNameValidator.violation(sanitized).ifPresent(still -> {
                            throw new ResourceResolutionException(String.format(
                                    "Resource name '%s' remains invalid after sanitization ('%s'): %s.", name, sanitized, still));
                        });
                        if (reportedSanitizations.add(name)) {
                            LOGGER.warn("Resource name '{}' sanitized to '{}' ({}).", name, sanitized, violation);
                        }
                        yield sanitized;
                    }
                    case ERROR_HANDLER -> throw new ResourceResolutionException(String.format(
                            "Resource name '%s' resolved from topic '%s' is not a valid Elasticsearch name: %s.",
                            name, record.topicName(), violation));
                    case FAIL -> throw new DebeziumException(String.format(
                            "Resource name '%s' resolved from topic '%s' is not a valid Elasticsearch name: %s. Set '%s' to 'sanitize' "
                                    + "or 'error_handler' to handle this without stopping the task.",
                            name, record.topicName(), violation, ElasticsearchSinkConnectorConfig.RESOURCE_NAME_INVALID_HANDLING));
                })
                .orElse(name);
    }
}
