/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.elasticsearch.ElasticsearchSinkTaskTestContext.ReportedRecord;
import io.debezium.connector.elasticsearch.junit.ElasticsearchTestCluster;
import io.debezium.connector.elasticsearch.junit.jupiter.ElasticsearchExtension;
import io.debezium.data.Envelope;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.GetResponse;

/**
 * The compatible-evolution matrix of DDD-61 6.3 asserted row by row against a live cluster:
 * additive changes are absorbed, removals are harmless, and type changes are reported as a
 * conflict naming reindexing as the remedy rather than retried forever.
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@ExtendWith(ElasticsearchExtension.class)
public class ElasticsearchSinkSchemaEvolutionIT {

    private static final String TOPIC = "evolution.customers";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("Key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().name("Source")
            .field("ts_ms", Schema.INT64_SCHEMA).field("db", Schema.STRING_SCHEMA).field("table", Schema.STRING_SCHEMA).build();
    private static final Schema ADDRESS_SCHEMA = SchemaBuilder.struct().name("Address").optional()
            .field("city", Schema.STRING_SCHEMA).build();

    private static final Schema BASE = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("name", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema WITH_EMAIL = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("name", Schema.OPTIONAL_STRING_SCHEMA).field("email", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema ID_ONLY = SchemaBuilder.struct().name("Value").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema AMOUNT_INT32 = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("amount", Schema.INT32_SCHEMA).build();
    private static final Schema AMOUNT_INT64 = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("amount", Schema.INT64_SCHEMA).build();
    private static final Schema NAME_REQUIRED = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("name", Schema.STRING_SCHEMA).build();
    private static final Schema WITH_ADDRESS = SchemaBuilder.struct().name("Value")
            .field("id", Schema.INT32_SCHEMA).field("name", Schema.OPTIONAL_STRING_SCHEMA).field("address", ADDRESS_SCHEMA).build();

    private ElasticsearchTestCluster cluster;
    private ElasticsearchSinkConnectorTask task;
    private ElasticsearchSinkTaskTestContext context;

    @BeforeEach
    void beforeEach(ElasticsearchTestCluster cluster) throws IOException {
        this.cluster = cluster;
        cluster.reset(List.of(TOPIC));
    }

    @AfterEach
    void afterEach() {
        if (task != null) {
            task.stop();
        }
    }

    @ParameterizedTest(name = "mapping.mode={0}")
    @ValueSource(strings = { "create_if_absent", "overwrite" })
    void shouldAbsorbAnAddedField(String mappingMode) throws IOException {
        // DDD-61 6.3 says a field added is "regenerated and applied on next resolution; the new
        // field is mapped from its Connect type" and that "additive changes are absorbed": the
        // second document must index with the new field and nothing may reach the reporter.
        startTask(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_MODE, mappingMode));

        put(record(BASE, new Struct(BASE).put("id", 1).put("name", "alice"), 0));
        put(record(WITH_EMAIL, new Struct(WITH_EMAIL).put("id", 2).put("name", "bob").put("email", "bob@example.com"), 1));

        assertThat(context.reportedRecords()).extracting(r -> r.error().getMessage()).as("no DLQ hit on an added column").isEmpty();
        assertThat(document("2")).containsEntry("email", "bob@example.com");
        assertThat(mapping().get("email")._kind()).isEqualTo(Property.Kind.Text);
    }

    @Test
    void shouldIndexARecordWithoutARemovedFieldAndKeepTheMapping() throws IOException {
        startTask(Map.of());

        put(record(BASE, new Struct(BASE).put("id", 1).put("name", "alice"), 0));
        put(record(ID_ONLY, new Struct(ID_ONLY).put("id", 2), 1));

        assertThat(context.reportedRecords()).isEmpty();
        assertThat(document("2")).containsEntry("id", 2).doesNotContainKey("name");
        assertThat(mapping()).as("Elasticsearch retains the mapping for absent fields").containsKey("name");
    }

    @Test
    void shouldReportAWidenedTypeAsAConflictNamingReindexing() throws IOException {
        // DDD-61 6.3 says a widened type "is detected as a conflict and reported ... naming the
        // field, the existing type and the incoming type", with reindexing as the remedy.
        startTask(Map.of());

        put(record(AMOUNT_INT32, new Struct(AMOUNT_INT32).put("id", 1).put("amount", 10), 0));
        final SinkRecord widened = record(AMOUNT_INT64, new Struct(AMOUNT_INT64).put("id", 2).put("amount", 1L << 40), 1);
        task.put(List.of(widened));
        final String diagnostic = conflictDiagnostic();

        assertThat(diagnostic).as("names the field").contains("amount");
        assertThat(diagnostic).as("names the remedy").contains("reindex");
        assertThat(mapping().get("amount")._kind()).as("the existing mapping is left alone").isEqualTo(Property.Kind.Integer);
    }

    @Test
    void shouldTreatAFieldMadeOptionalAsNoMappingChange() throws IOException {
        startTask(Map.of());

        put(record(NAME_REQUIRED, new Struct(NAME_REQUIRED).put("id", 1).put("name", "alice"), 0));
        put(record(BASE, new Struct(BASE).put("id", 2).put("name", null), 1));

        assertThat(context.reportedRecords()).isEmpty();
        assertThat(document("1")).containsEntry("name", "alice");
        assertThat(document("2")).as("null.value.handling=omit").doesNotContainKey("name");
        assertThat(mapping().get("name")._kind()).isEqualTo(Property.Kind.Text);
    }

    @Test
    void shouldMapANestedStructAsAnObject() throws IOException {
        startTask(Map.of());

        put(record(WITH_ADDRESS, new Struct(WITH_ADDRESS).put("id", 1).put("name", "alice")
                .put("address", new Struct(ADDRESS_SCHEMA).put("city", "Paris")), 0));

        assertThat(context.reportedRecords()).isEmpty();
        assertThat(document("1")).containsEntry("address", Map.of("city", "Paris"));
        final Property address = mapping().get("address");
        assertThat(address._kind()).isEqualTo(Property.Kind.Object);
        assertThat(address.object().properties().get("city")._kind()).isEqualTo(Property.Kind.Text);
    }

    @Test
    void shouldRouteAnUnmappedFieldToTheReporterWhenMappingIsLeftToElasticsearch() throws IOException {
        // DDD-61 6.3: under mapping.mode=none the mapping is the user's, and a strict one routes
        // the record carrying an unmapped field to the error handler naming that field (the
        // connector applies no 'mapping.dynamic' of its own here; DDD-variants entry 14).
        cluster.client().indices().create(c -> c.index(TOPIC).mappings(m -> m
                .dynamic(co.elastic.clients.elasticsearch._types.mapping.DynamicMapping.Strict)
                .properties("id", p -> p.integer(i -> i))
                .properties("name", p -> p.keyword(k -> k))));
        startTask(Map.of(ElasticsearchSinkConnectorConfig.MAPPING_MODE, "none"));

        put(record(BASE, new Struct(BASE).put("id", 1).put("name", "alice"), 0));
        final SinkRecord added = record(WITH_EMAIL, new Struct(WITH_EMAIL).put("id", 2).put("name", "bob").put("email", "bob@example.com"), 1);
        put(added);

        assertThat(context.reportedRecords()).hasSize(1);
        final ReportedRecord reported = context.reportedRecords().get(0);
        assertThat(reported.record()).isSameAs(added);
        assertThat(reported.error()).hasMessageContaining("strict_dynamic_mapping_exception").hasMessageContaining("email");
        assertThat(exists("2")).isFalse();
    }

    /**
     * The widened-type diagnostic, wherever the connector surfaces it: on the reporter, or as the
     * failure the task stores and rethrows on its next put.
     */
    private String conflictDiagnostic() {
        if (!context.reportedRecords().isEmpty()) {
            return context.reportedRecords().get(0).error().getMessage();
        }
        final Throwable[] failure = new Throwable[1];
        assertThatThrownBy(() -> task.put(List.of())).isInstanceOf(ConnectException.class).satisfies(e -> failure[0] = e);
        final StringBuilder message = new StringBuilder();
        for (Throwable t = failure[0]; t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(' ');
        }
        return message.toString();
    }

    private void put(SinkRecord record) {
        task.put(List.of(record));
        task.preCommit(Map.of(PARTITION, new OffsetAndMetadata(0)));
    }

    private void startTask(Map<String, String> overrides) {
        final Map<String, String> properties = new HashMap<>(cluster.connectionProperties());
        properties.put("name", "evolution");
        properties.put("errors.tolerance", "all");
        properties.putAll(overrides);

        context = new ElasticsearchSinkTaskTestContext(properties);
        task = new ElasticsearchSinkConnectorTask();
        task.initialize(context);
        task.start(properties);
        task.open(List.of(PARTITION));
    }

    private static SinkRecord record(Schema valueSchema, Struct value, long offset) {
        final Envelope envelope = Envelope.defineSchema().withName(TOPIC + ".Envelope").withRecord(valueSchema).withSource(SOURCE_SCHEMA).build();
        final Struct source = new Struct(SOURCE_SCHEMA).put("ts_ms", Instant.now().toEpochMilli()).put("db", "inventory").put("table", "customers");
        final Struct key = new Struct(KEY_SCHEMA).put("id", value.getInt32("id"));
        return new SinkRecord(TOPIC, 0, KEY_SCHEMA, key, envelope.schema(), envelope.create(value, source, Instant.now()), offset);
    }

    private Map<String, Property> mapping() throws IOException {
        return cluster.client().indices().getMapping(g -> g.index(TOPIC)).result().get(TOPIC).mappings().properties();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> document(String id) throws IOException {
        final GetResponse<Map> response = cluster.client().get(g -> g.index(TOPIC).id(id), Map.class);
        assertThat(response.found()).as("document %s in %s", id, TOPIC).isTrue();
        return response.source();
    }

    private boolean exists(String id) throws IOException {
        return cluster.client().exists(e -> e.index(TOPIC).id(id)).value();
    }
}
