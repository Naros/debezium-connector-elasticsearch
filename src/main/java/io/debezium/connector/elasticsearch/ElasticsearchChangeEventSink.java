/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.debezium.connector.elasticsearch.bulk.AdaptiveThrottle;
import io.debezium.connector.elasticsearch.bulk.ElasticsearchBulkWriter;
import io.debezium.connector.elasticsearch.bulk.ElasticsearchBulkWriter.BulkItem;
import io.debezium.connector.elasticsearch.client.ClusterVersionHandshake;
import io.debezium.connector.elasticsearch.client.ElasticsearchConnection;
import io.debezium.connector.elasticsearch.convert.DocumentConverter;
import io.debezium.connector.elasticsearch.mapping.MappingManager;
import io.debezium.connector.elasticsearch.metrics.ElasticsearchSinkConnectorMetrics;
import io.debezium.connector.elasticsearch.naming.ElasticsearchCollectionNamingStrategy;
import io.debezium.connector.elasticsearch.naming.ResourceResolutionException;
import io.debezium.connector.elasticsearch.record.DocumentIdStrategy;
import io.debezium.connector.elasticsearch.record.ElasticsearchSinkRecord;
import io.debezium.connector.elasticsearch.record.RecordAdapter;
import io.debezium.connector.elasticsearch.record.RecordProcessingException;
import io.debezium.connector.elasticsearch.record.SinkOperation;
import io.debezium.dlq.ErrorReporter;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.AbstractChangeEventSink;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Strings;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;

/**
 * The {@link ChangeEventSink} for Elasticsearch, operating on batches the shared
 * {@code DeduplicatingBuffer} has already reduced to one write per resource and {@code _id}:
 * each record is adapted to an operation, given its identity and resource, converted with
 * logical-type fidelity, and issued one bulk request at a time. Truncates flush the accumulated
 * items first and are waited on to completion, preserving the one-request-in-flight invariant.
 *
 * @author Chris Cranford
 */
public class ElasticsearchChangeEventSink extends AbstractChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchChangeEventSink.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long DELETE_BYTES_ESTIMATE = 96;

    private final ElasticsearchSinkConnectorConfig sinkConfig;
    private final ElasticsearchConnection connection;
    private final ErrorReporter errorReporter;
    private final ElasticsearchSinkConnectorMetrics metrics;
    private final RecordAdapter recordAdapter;
    private final DocumentIdStrategy idStrategy;
    private final DocumentConverter converter;
    private final MappingManager mappingManager;
    private final TruncateHandler truncateHandler;
    private final ElasticsearchBulkWriter bulkWriter;
    private final ElasticsearchCollectionNamingStrategy expressionResolver;
    private final boolean estimateSizes;

    private long lastDrainMs = System.currentTimeMillis();

    public ElasticsearchChangeEventSink(ElasticsearchSinkConnectorConfig config, ElasticsearchConnection connection,
                                        ErrorReporter errorReporter, ElasticsearchSinkConnectorMetrics metrics,
                                        Map<String, String> rawProperties) {
        super(config, errorReporter);
        this.sinkConfig = config;
        this.connection = connection;
        this.errorReporter = errorReporter;
        this.metrics = metrics;
        this.recordAdapter = new RecordAdapter(config);
        this.idStrategy = new DocumentIdStrategy(config);
        this.converter = new DocumentConverter(config);
        this.mappingManager = new MappingManager(connection.client(), config, config.getConnectorName());
        this.truncateHandler = new TruncateHandler(connection.client(), config, mappingManager);
        this.bulkWriter = new ElasticsearchBulkWriter(connection.client(), config,
                new AdaptiveThrottle(config.getBatchSize(), config.maxRequestsPerSecond(), config.maxBytesPerSecond()),
                errorReporter, metrics, connection.handshake());
        this.expressionResolver = new ElasticsearchCollectionNamingStrategy();
        this.expressionResolver.configure(rawProperties);
        this.estimateSizes = config.bulkSizeBytes() > 0 || config.maxBytesPerSecond() != null;

        final ClusterVersionHandshake.ClusterVersion version = connection.handshake().ensureProbed();
        metrics.connectionResolved(version.number(), sinkConfig.apiCompatibilityMode().getValue());
        validateIngestPipeline();
    }

    @Override
    public CollectionId getCollectionId(String collectionName) {
        return collectionName == null ? null : new CollectionId(collectionName);
    }

    @Override
    protected DebeziumSinkRecord createSinkRecord(SinkRecord kafkaSinkRecord) {
        return new ElasticsearchSinkRecord(kafkaSinkRecord, sinkConfig.cloudEventsSchemaNamePattern());
    }

    /**
     * Processes one delivery from the runtime adapter, then drains partial batches per
     * {@code linger.ms}; the runtime's flush callback drains unconditionally through
     * {@link #flushAll()} so offsets are committed only for confirmed records.
     */
    public void execute(Collection<SinkRecord> records) {
        for (Batch batch : put(records)) {
            writeBatch(batch);
        }
        final long lingerMs = sinkConfig.lingerMs();
        if (lingerMs <= 0 || System.currentTimeMillis() - lastDrainMs >= lingerMs) {
            flushAll();
        }
    }

    /**
     * Drains and writes everything still buffered; invoked before offset commit.
     */
    public void flushAll() {
        forceFlush();
        lastDrainMs = System.currentTimeMillis();
    }

    @Override
    public CollectionId getCollectionIdFromRecord(DebeziumSinkRecord record) {
        try {
            return super.getCollectionIdFromRecord(record);
        }
        catch (ResourceResolutionException e) {
            // A record-level naming failure (unresolvable placeholder, invalid name under
            // 'error_handler') is routed to the error reporter, not silently skipped and not fatal.
            reportRecord(record, e);
            return null;
        }
    }

    @Override
    protected void doWriteBatch(Batch batch) {
        final List<BulkItem> items = new ArrayList<>();
        for (BatchRecord batchRecord : batch) {
            final DebeziumSinkRecord record = batchRecord.record();
            if (record.isTruncate()) {
                // DDD-61 5.1 sequencing: drain the in-flight work, then execute the truncate to
                // completion, and only then build the next bulk request.
                writeItems(items);
                executeTruncate(batchRecord);
                continue;
            }
            try {
                final SinkOperation operation = recordAdapter.adapt(record);
                if (operation instanceof SinkOperation.Skip skip) {
                    LOGGER.debug("Skipped record from topic '{}' partition {} offset {}: {}",
                            record.topicName(), record.partition(), record.offset(), skip.reason());
                    metrics.filtered();
                }
                else if (operation instanceof SinkOperation.Delete) {
                    if (sinkConfig.isDeleteEnabled()) {
                        items.add(buildDelete(batchRecord));
                    }
                    else {
                        metrics.filtered();
                    }
                }
                else if (operation instanceof SinkOperation.Write write) {
                    items.add(buildWrite(batchRecord, write));
                }
                else if (operation instanceof SinkOperation.Truncate) {
                    writeItems(items);
                    executeTruncate(batchRecord);
                }
            }
            catch (RecordProcessingException e) {
                reportRecord(record, e);
            }
        }
        writeItems(items);
    }

    private void writeItems(List<BulkItem> items) {
        if (items.isEmpty()) {
            return;
        }
        bulkWriter.write(List.copyOf(items));
        items.clear();
    }

    private void executeTruncate(BatchRecord batchRecord) {
        try {
            truncateHandler.truncate(batchRecord.record(), batchRecord.collectionId().name());
            metrics.truncated();
        }
        catch (RecordProcessingException e) {
            reportRecord(batchRecord.record(), e);
        }
    }

    private BulkItem buildDelete(BatchRecord batchRecord) {
        final DebeziumSinkRecord record = batchRecord.record();
        final String resource = batchRecord.collectionId().name();
        final String id = idStrategy.documentId(record).orElseThrow(() -> new RecordProcessingException(String.format(
                "A delete for topic '%s' partition %s offset %s has no document identity under 'primary.key.mode=none'.",
                record.topicName(), record.partition(), record.offset())));

        mappingManager.ensureResource(resource, null);
        final BulkOperation operation = BulkOperation.of(b -> b.delete(d -> d.index(resource).id(id)));
        return new BulkItem(batchRecord, operation, true, DELETE_BYTES_ESTIMATE);
    }

    private BulkItem buildWrite(BatchRecord batchRecord, SinkOperation.Write write) {
        final DebeziumSinkRecord record = batchRecord.record();
        final String resource = batchRecord.collectionId().name();
        final Optional<String> id = idStrategy.documentId(record);
        final Map<String, Object> document = converter.convert(write);
        final String routing = sinkConfig.indexRoutingField() != null
                ? converter.routingValue(record, document, sinkConfig.indexRoutingField())
                : null;
        final String pipeline = resolvePipeline(record);
        final WriteMethod method = sinkConfig.writeMethodFor(write.operation());
        if (method.usesUpdateApi() && id.isEmpty()) {
            // Unreachable while validation rejects 'primary.key.mode=none' with the update API,
            // but a descriptive failure beats a bare NoSuchElementException if that ever changes.
            throw new RecordProcessingException(String.format(
                    "A '%s' write for topic '%s' partition %s offset %s has no document identity under 'primary.key.mode=none'.",
                    method.getValue(), record.topicName(), record.partition(), record.offset()));
        }

        mappingManager.ensureResource(resource, write.payload() != null ? write.payload().schema() : null);

        final BulkOperation operation = switch (method) {
            case INDEX -> BulkOperation.of(b -> b.index(i -> {
                i.index(resource).document(document);
                id.ifPresent(i::id);
                if (routing != null) {
                    i.routing(routing);
                }
                if (pipeline != null) {
                    i.pipeline(pipeline);
                }
                return i;
            }));
            case CREATE -> BulkOperation.of(b -> b.create(c -> {
                c.index(resource).document(document);
                id.ifPresent(c::id);
                if (routing != null) {
                    c.routing(routing);
                }
                if (pipeline != null) {
                    c.pipeline(pipeline);
                }
                return c;
            }));
            case UPSERT -> BulkOperation.of(b -> b.update(u -> {
                u.index(resource).id(id.orElseThrow());
                if (routing != null) {
                    u.routing(routing);
                }
                return u.action(a -> a.doc(document).docAsUpsert(true));
            }));
            case UPDATE -> BulkOperation.of(b -> b.update(u -> {
                u.index(resource).id(id.orElseThrow());
                if (routing != null) {
                    u.routing(routing);
                }
                return u.action(a -> a.doc(document));
            }));
        };
        return new BulkItem(batchRecord, operation, false, estimateBytes(document));
    }

    private String resolvePipeline(DebeziumSinkRecord record) {
        final String pipeline = sinkConfig.ingestPipeline();
        if (Strings.isNullOrBlank(pipeline)) {
            return null;
        }
        return pipeline.contains("${") ? expressionResolver.resolveExpression(record, pipeline) : pipeline;
    }

    private long estimateBytes(Map<String, Object> document) {
        if (!estimateSizes) {
            return 0;
        }
        try {
            return JSON.writeValueAsBytes(document).length + 64;
        }
        catch (Exception e) {
            return 512;
        }
    }

    private void reportRecord(DebeziumSinkRecord record, Exception failure) {
        LOGGER.warn("Routing record from topic '{}' partition {} offset {} to the error reporter: {}",
                record.topicName(), record.partition(), record.offset(), failure.getMessage());
        errorReporter.report(record, failure);
        metrics.errantRecordsReported(1);
    }

    private void validateIngestPipeline() {
        final String pipeline = sinkConfig.ingestPipeline();
        if (Strings.isNullOrBlank(pipeline) || !sinkConfig.isIngestPipelineValidate() || pipeline.contains("${")) {
            return;
        }
        try {
            connection.client().ingest().getPipeline(g -> g.id(pipeline));
        }
        catch (Exception e) {
            throw new ConnectException(String.format(
                    "The ingest pipeline '%s' named by '%s' does not exist (or could not be verified). Create it, or set "
                            + "'%s' to false.",
                    pipeline, ElasticsearchSinkConnectorConfig.INGEST_PIPELINE,
                    ElasticsearchSinkConnectorConfig.INGEST_PIPELINE_VALIDATE), e);
        }
    }

    /**
     * Task-fatal and framework-retriable failures must propagate to the task rather than being
     * re-driven into the error reporter; everything else is isolated record by record by the base
     * class so a single bad record cannot discard healthy ones.
     */
    @Override
    protected boolean isRetriableWriteException(RuntimeException exception) {
        return exception instanceof ConnectException;
    }

    @Override
    public void close() {
        // The connection is owned and closed by the task, which also serves the Debezium Server
        // adapter delegating to it.
    }
}
